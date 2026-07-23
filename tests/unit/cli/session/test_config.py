from datetime import timedelta
from pathlib import Path

import jsonschema
import pytest
import yaml
from gds_cli.session.config import JOB_CONFIG_ENV_VAR, JobsConfig, parse_ttl
from pydantic import ValidationError

VALID_CONFIG = {
    "session": {"memory": "2GB", "ttl": "30m"},
    "jobs": [
        {
            "project": {"type": "cypher", "query": "RETURN gds.graph.project.remote(n, m)"},
            "compute": [{"compute": "louvain", "config": {"resultProperty": "community"}}],
            "write": [{"nodeProperty": "community"}],
        }
    ],
}


def test_job_config_from_data_valid() -> None:
    cfg = JobsConfig._from_data(VALID_CONFIG)

    assert cfg.session.memory == "2GB"
    job = cfg.jobs[0]
    assert job.project.type == "cypher"
    assert job.compute[0].algorithm == "louvain"
    assert job.compute[0].result_property == "community"
    assert job.write[0].node_property == "community"
    assert job.write[0].target == "community"


def test_session_name_is_optional() -> None:
    # VALID_CONFIG has no session.name
    assert JobsConfig._from_data(VALID_CONFIG).session.name is None


def test_session_name_is_parsed_when_present() -> None:
    data = {**VALID_CONFIG, "session": {"name": "warm", "memory": "2GB", "ttl": "30m"}}

    assert JobsConfig._from_data(data).session.name == "warm"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("30m", timedelta(minutes=30)),
        ("2h", timedelta(hours=2)),
        ("1d", timedelta(days=1)),
        ("45", timedelta(minutes=45)),  # bare string -> minutes
        (90, timedelta(minutes=90)),  # int -> minutes
        ("1H", timedelta(hours=1)),  # case-insensitive
    ],
)
def test_parse_ttl_accepts_durations_and_minutes(value: "str | int", expected: timedelta) -> None:
    assert parse_ttl(value) == expected


@pytest.mark.parametrize("value", ["", "30x", "m", "1.5h", 0, "0m", True])
def test_parse_ttl_rejects_bad_values(value: "str | int") -> None:
    with pytest.raises(ValueError):
        parse_ttl(value)


def test_session_ttl_parsed_from_suffixed_string() -> None:
    data = {**VALID_CONFIG, "session": {"memory": "2GB", "ttl": "2h"}}

    assert JobsConfig._from_data(data).session.ttl == timedelta(hours=2)


def test_session_ttl_accepts_integer_minutes() -> None:
    data = {**VALID_CONFIG, "session": {"memory": "2GB", "ttl": 30}}

    assert JobsConfig._from_data(data).session.ttl == timedelta(minutes=30)


def test_session_ttl_rejects_bad_string_at_schema() -> None:
    data = {**VALID_CONFIG, "session": {"memory": "2GB", "ttl": "30x"}}

    with pytest.raises((ValidationError, jsonschema.exceptions.ValidationError)):
        JobsConfig._from_data(data)


def test_compute_parameters_exclude_result_property() -> None:
    data = {
        **VALID_CONFIG,
        "jobs": [
            {
                "project": {"type": "cypher", "query": "q"},
                "compute": [{"compute": "pageRank", "config": {"resultProperty": "pagerank", "maxIterations": 20}}],
                "write": [{"nodeProperty": "pagerank"}],
            }
        ],
    }

    cfg = JobsConfig._from_data(data)
    spec = cfg.jobs[0].compute[0]

    assert spec.result_property == "pagerank"
    assert spec.parameters == {"maxIterations": 20}


def test_write_property_rename_target() -> None:
    data = {
        **VALID_CONFIG,
        "jobs": [
            {
                "project": {"type": "cypher", "query": "q"},
                "compute": [{"compute": "pageRank", "config": {"resultProperty": "pagerank"}}],
                "write": [{"nodeProperty": "pagerank", "writeProperty": "rank"}],
            }
        ],
    }

    cfg = JobsConfig._from_data(data)

    assert cfg.jobs[0].write[0].target == "rank"


def test_legacy_flat_schema_gives_clear_error() -> None:
    legacy = {
        "session": {"name": "s", "memory": "2GB", "ttl": "30m"},
        "projections": [{"graph_name": "social", "query": "q"}],
        "algorithms": [{"name": "louvain", "graph_name": "social", "mode": "mutate", "mutate_property": "c"}],
    }

    with pytest.raises(ValueError, match="retired flat schema"):
        JobsConfig._from_data(legacy)


def test_job_config_rejects_unknown_top_level_field() -> None:
    data = {**VALID_CONFIG, "extra": "nope"}

    with pytest.raises(jsonschema.exceptions.ValidationError):
        JobsConfig._from_data(data)


def test_compute_requires_result_property() -> None:
    data = {
        **VALID_CONFIG,
        "jobs": [
            {
                "project": {"type": "cypher", "query": "q"},
                "compute": [{"compute": "louvain", "config": {"maxIterations": 5}}],
                "write": [],
            }
        ],
    }

    with pytest.raises(jsonschema.exceptions.ValidationError):
        JobsConfig._from_data(data)


def test_mutate_must_reference_a_produced_property() -> None:
    data = {
        **VALID_CONFIG,
        "jobs": [
            {
                "project": {"type": "cypher", "query": "q"},
                "compute": [{"compute": "louvain", "config": {"resultProperty": "community"}}],
                "mutate": [{"nodeProperty": "not-produced"}],
            }
        ],
    }

    with pytest.raises(ValidationError, match="mutate references property 'not-produced'"):
        JobsConfig._from_data(data)


def test_write_must_reference_a_produced_property() -> None:
    data = {
        **VALID_CONFIG,
        "jobs": [
            {
                "project": {"type": "cypher", "query": "q"},
                "compute": [{"compute": "louvain", "config": {"resultProperty": "community"}}],
                "write": [{"nodeProperty": "not-produced"}],
            }
        ],
    }

    with pytest.raises(ValidationError, match="write references property 'not-produced'"):
        JobsConfig._from_data(data)


def test_mutated_properties_auto_derived_from_downstream_feature() -> None:
    data = {
        **VALID_CONFIG,
        "jobs": [
            {
                "project": {"type": "cypher", "query": "q"},
                "compute": [
                    {"compute": "pageRank", "config": {"resultProperty": "pagerank"}},
                    {"compute": "fastRP", "config": {"resultProperty": "embedding", "featureProperties": ["pagerank"]}},
                ],
                "write": [{"nodeProperty": "embedding"}],
            }
        ],
    }

    cfg = JobsConfig._from_data(data)

    # pagerank is materialized because FastRP names it in featureProperties - no `mutate` needed.
    assert cfg.jobs[0].mutated_properties == {"pagerank"}


def test_mutated_properties_empty_without_downstream_consumption() -> None:
    # VALID_CONFIG is a single louvain compute; nothing consumes `community`.
    cfg = JobsConfig._from_data(VALID_CONFIG)

    assert cfg.jobs[0].mutated_properties == set()


def test_explicit_mutate_unions_with_derived() -> None:
    data = {
        **VALID_CONFIG,
        "jobs": [
            {
                "project": {"type": "cypher", "query": "q"},
                "compute": [
                    {"compute": "pageRank", "config": {"resultProperty": "pagerank"}},
                    {"compute": "fastRP", "config": {"resultProperty": "embedding", "featureProperties": ["pagerank"]}},
                ],
                "mutate": [{"nodeProperty": "embedding"}],
                "write": [{"nodeProperty": "embedding"}],
            }
        ],
    }

    cfg = JobsConfig._from_data(data)

    # pagerank auto-derived (FastRP feature) + embedding forced via explicit mutate.
    assert cfg.jobs[0].mutated_properties == {"pagerank", "embedding"}


def test_cypher_projection_rejects_native_fields() -> None:
    data = {
        **VALID_CONFIG,
        "jobs": [
            {
                "project": {"type": "cypher", "query": "q", "nodeLabels": ["Person"]},
                "compute": [{"compute": "louvain", "config": {"resultProperty": "community"}}],
            }
        ],
    }

    # JSON schema (additionalProperties allows the key, but the oneOf/native combo is
    # caught by the pydantic model validator).
    with pytest.raises((ValidationError, jsonschema.exceptions.ValidationError)):
        JobsConfig._from_data(data)


def test_native_projection_requires_labels_and_types() -> None:
    data = {
        **VALID_CONFIG,
        "jobs": [
            {
                "project": {"type": "native", "nodeLabels": ["Person"]},  # missing relationshipTypes
                "compute": [{"compute": "louvain", "config": {"resultProperty": "community"}}],
            }
        ],
    }

    with pytest.raises((ValidationError, jsonschema.exceptions.ValidationError)):
        JobsConfig._from_data(data)


def test_native_projection_parses() -> None:
    data = {
        **VALID_CONFIG,
        "jobs": [
            {
                "project": {"type": "native", "nodeLabels": ["Person"], "relationshipTypes": ["KNOWS"]},
                "compute": [{"compute": "wcc", "config": {"resultProperty": "componentId"}}],
                "write": [{"nodeProperty": "componentId"}],
            }
        ],
    }

    cfg = JobsConfig._from_data(data)
    project = cfg.jobs[0].project

    assert project.is_native
    assert project.node_labels == ["Person"]
    assert project.relationship_types == ["KNOWS"]


STANDALONE_CONFIG = {
    "session": {"memory": "2GB", "ttl": "30m", "cloud": "gcp", "region": "europe-west1"},
    "jobs": [
        {
            "project": {"type": "construct", "file": "social-network.json"},
            "compute": [{"compute": "pageRank", "config": {"resultProperty": "pagerank"}}],
            "write": [{"nodeProperty": "pagerank", "outputFile": "pagerank.csv"}],
        }
    ],
}


def test_standalone_session_detected_from_cloud_region() -> None:
    cfg = JobsConfig._from_data(STANDALONE_CONFIG)

    assert cfg.session.is_standalone
    assert cfg.session.cloud == "gcp"
    assert cfg.session.region == "europe-west1"
    assert cfg.jobs[0].project.is_construct
    assert cfg.jobs[0].project.file == "social-network.json"
    assert cfg.jobs[0].write[0].output_file == "pagerank.csv"


def test_standalone_write_requires_output_file() -> None:
    data = {
        **STANDALONE_CONFIG,
        "jobs": [
            {
                "project": {"type": "construct", "file": "g.json"},
                "compute": [{"compute": "pageRank", "config": {"resultProperty": "pagerank"}}],
                "write": [{"nodeProperty": "pagerank"}],  # no `outputFile`
            }
        ],
    }

    with pytest.raises(ValidationError, match="needs an `outputFile`"):
        JobsConfig._from_data(data)


def test_standalone_jobs_reject_shared_output_file() -> None:
    # Two jobs streaming to the same file would clobber each other - rejected up front.
    data = {
        **STANDALONE_CONFIG,
        "jobs": [
            {
                "project": {"type": "construct", "file": "g.json"},
                "compute": [{"compute": "pageRank", "config": {"resultProperty": "pagerank"}}],
                "write": [{"nodeProperty": "pagerank", "outputFile": "result.json"}],
            },
            {
                "project": {"type": "construct", "file": "g.json"},
                "compute": [{"compute": "louvain", "config": {"resultProperty": "community"}}],
                "write": [{"nodeProperty": "community", "outputFile": "./result.json"}],  # same path
            },
        ],
    }

    with pytest.raises(ValidationError, match="already used by job 0"):
        JobsConfig._from_data(data)


def test_standalone_single_job_may_share_output_file_across_writes() -> None:
    # Within ONE job, several properties sharing a file is allowed (grouped together).
    data = {
        **STANDALONE_CONFIG,
        "jobs": [
            {
                "project": {"type": "construct", "file": "g.json"},
                "compute": [
                    {"compute": "pageRank", "config": {"resultProperty": "pagerank"}},
                    {"compute": "louvain", "config": {"resultProperty": "community"}},
                ],
                "write": [
                    {"nodeProperty": "pagerank", "outputFile": "result.json"},
                    {"nodeProperty": "community", "outputFile": "result.json"},
                ],
            }
        ],
    }

    cfg = JobsConfig._from_data(data)

    assert [w.output_file for w in cfg.jobs[0].write] == ["result.json", "result.json"]


def test_attached_session_rejects_output_file() -> None:
    data = {
        **VALID_CONFIG,
        "jobs": [
            {
                "project": {"type": "cypher", "query": "q"},
                "compute": [{"compute": "louvain", "config": {"resultProperty": "community"}}],
                "write": [{"nodeProperty": "community", "outputFile": "x.csv"}],
            }
        ],
    }

    with pytest.raises(ValidationError, match="only applies to standalone"):
        JobsConfig._from_data(data)


def test_attached_session_is_not_standalone() -> None:
    assert JobsConfig._from_data(VALID_CONFIG).session.is_standalone is False


def test_session_requires_both_cloud_and_region() -> None:
    data = {**STANDALONE_CONFIG, "session": {"memory": "2GB", "ttl": "30m", "cloud": "gcp"}}

    with pytest.raises((ValidationError, jsonschema.exceptions.ValidationError)):
        JobsConfig._from_data(data)


def test_standalone_session_requires_construct_projection() -> None:
    data = {
        **STANDALONE_CONFIG,
        "jobs": [
            {
                "project": {"type": "cypher", "query": "q"},
                "compute": [{"compute": "pageRank", "config": {"resultProperty": "pagerank"}}],
            }
        ],
    }

    with pytest.raises(ValidationError, match="has no database"):
        JobsConfig._from_data(data)


def test_construct_projection_requires_file() -> None:
    data = {
        **VALID_CONFIG,
        "jobs": [
            {
                "project": {"type": "construct"},
                "compute": [{"compute": "louvain", "config": {"resultProperty": "community"}}],
            }
        ],
    }

    with pytest.raises((ValidationError, jsonschema.exceptions.ValidationError)):
        JobsConfig._from_data(data)


def test_job_config_from_file(tmp_path: Path) -> None:
    config_path = tmp_path / "job.yaml"
    config_path.write_text(yaml.safe_dump(VALID_CONFIG))

    cfg = JobsConfig.from_file(config_path)

    assert cfg.jobs[0].project.type == "cypher"


def test_job_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(JOB_CONFIG_ENV_VAR, yaml.safe_dump(VALID_CONFIG))

    cfg = JobsConfig.from_env()

    assert cfg.session.ttl == timedelta(minutes=30)


def test_job_config_from_env_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(JOB_CONFIG_ENV_VAR, raising=False)

    with pytest.raises(RuntimeError, match="GDS_JOB_CONFIG"):
        JobsConfig.from_env()
