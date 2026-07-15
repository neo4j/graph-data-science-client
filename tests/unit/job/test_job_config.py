import pytest

from graphdatascience.job import JobConfig, JobConfigValidationError, NativeProjection, WriteOrMutateMode

VALID_YAML = """
metadata:
  name: my-job
  auraInstanceId: abc123
projections:
  - "MATCH (n) RETURN n"
algorithms:
  - name: pageRank
    mode: stream
"""


def test_from_yaml_file_parses_into_typed_object(tmp_path) -> None:  # type: ignore[no-untyped-def]
    config_file = tmp_path / "job-config.yaml"
    config_file.write_text(VALID_YAML)

    job_config = JobConfig.from_yaml_file(config_file)

    assert job_config.metadata.name == "my-job"
    assert job_config.metadata.aura_instance_id == "abc123"
    assert job_config.metadata.version is None
    assert job_config.projections == ["MATCH (n) RETURN n"]
    assert job_config.algorithms[0].name == "pageRank"
    assert job_config.algorithms[0].mode == "stream"
    assert job_config.algorithms[0].config == {}


def test_from_yaml_file_parses_native_projection_and_write_mode(tmp_path) -> None:  # type: ignore[no-untyped-def]
    yaml_content = """
metadata:
  name: my-job
  auraInstanceId: abc123
projections:
  - name: my-graph
    nodeLabels:
      - Person
    relationshipTypes:
      - KNOWS
algorithms:
  - name: pageRank
    mode:
      name: write
      property: rank
    config:
      dampingFactor: 0.85
"""
    config_file = tmp_path / "job-config.yaml"
    config_file.write_text(yaml_content)

    job_config = JobConfig.from_yaml_file(config_file)

    projection = job_config.projections[0]
    assert isinstance(projection, NativeProjection)
    assert projection.name == "my-graph"
    assert projection.node_labels == ["Person"]
    assert projection.relationship_types == ["KNOWS"]

    mode = job_config.algorithms[0].mode
    assert isinstance(mode, WriteOrMutateMode)
    assert mode.name == "write"
    assert mode.property == "rank"
    assert job_config.algorithms[0].config == {"dampingFactor": 0.85}


def test_from_yaml_file_rejects_missing_required_field(tmp_path) -> None:  # type: ignore[no-untyped-def]
    yaml_content = """
metadata:
  name: my-job
projections:
  - "MATCH (n) RETURN n"
algorithms:
  - name: pageRank
    mode: stream
"""
    config_file = tmp_path / "job-config.yaml"
    config_file.write_text(yaml_content)

    with pytest.raises(JobConfigValidationError, match="auraInstanceId"):
        JobConfig.from_yaml_file(config_file)


def test_from_yaml_file_rejects_empty_projections(tmp_path) -> None:  # type: ignore[no-untyped-def]
    yaml_content = """
metadata:
  name: my-job
  auraInstanceId: abc123
projections: []
algorithms:
  - name: pageRank
    mode: stream
"""
    config_file = tmp_path / "job-config.yaml"
    config_file.write_text(yaml_content)

    with pytest.raises(JobConfigValidationError, match="projections"):
        JobConfig.from_yaml_file(config_file)
