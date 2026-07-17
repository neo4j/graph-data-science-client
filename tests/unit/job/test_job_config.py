import pytest

from graphdatascience.job import (
    AlgorithmStep,
    CypherProjection,
    JobConfig,
    JobConfigValidationError,
    NativeProjection,
    WriteBackStep,
    WriteOrMutateMode,
)

VALID_YAML = """
projections:
  - native:
      graph: "g1"
  - cypher:
      graph: "g2"
      query: "MATCH (n) RETURN n"
steps:
  - name: "Run WCC"
    type: algorithm
    params:
      algorithm: "wcc"
      graph: "g1"
      configuration:
        concurrency: 4
      mode: "stream"
  - name: "Run PageRank"
    type: algorithm
    params:
      algorithm: "pagerank"
      graph: "g2"
      mode:
        name: "write"
        property: "rank"
  - name: "Write back"
    type: write-back
    params:
      graph: "g2"
      node_properties: ["rank"]
"""


def test_from_yaml_file_parses_into_typed_object(tmp_path) -> None:  # type: ignore[no-untyped-def]
    config_file = tmp_path / "job-config.yaml"
    config_file.write_text(VALID_YAML)

    job_config = JobConfig.from_yaml_file(config_file)

    assert isinstance(job_config.projections[0].spec, NativeProjection)
    assert job_config.projections[0].graph == "g1"

    cypher_projection = job_config.projections[1].spec
    assert isinstance(cypher_projection, CypherProjection)
    assert cypher_projection.graph == "g2"
    assert cypher_projection.query == "MATCH (n) RETURN n"

    wcc_step = job_config.steps[0]
    assert isinstance(wcc_step, AlgorithmStep)
    assert wcc_step.name == "Run WCC"
    assert wcc_step.params.algorithm == "wcc"
    assert wcc_step.params.graph == "g1"
    assert wcc_step.params.mode == "stream"
    assert wcc_step.params.configuration == {"concurrency": 4}

    page_rank_step = job_config.steps[1]
    assert isinstance(page_rank_step, AlgorithmStep)
    mode = page_rank_step.params.mode
    assert isinstance(mode, WriteOrMutateMode)
    assert mode.name == "write"
    assert mode.property == "rank"
    assert page_rank_step.params.configuration == {}

    write_back_step = job_config.steps[2]
    assert isinstance(write_back_step, WriteBackStep)
    assert write_back_step.params.graph == "g2"
    assert write_back_step.params.node_properties == ["rank"]


def test_from_yaml_file_rejects_projection_with_both_kinds(tmp_path) -> None:  # type: ignore[no-untyped-def]
    yaml_content = """
projections:
  - native:
      graph: "g1"
    cypher:
      graph: "g1"
      query: "MATCH (n) RETURN n"
steps:
  - name: "Run WCC"
    type: algorithm
    params:
      algorithm: "wcc"
      graph: "g1"
      mode: "stream"
"""
    config_file = tmp_path / "job-config.yaml"
    config_file.write_text(yaml_content)

    with pytest.raises(JobConfigValidationError, match="exactly one"):
        JobConfig.from_yaml_file(config_file)


def test_from_yaml_file_rejects_unknown_step_type(tmp_path) -> None:  # type: ignore[no-untyped-def]
    yaml_content = """
projections:
  - native:
      graph: "g1"
steps:
  - name: "Mystery"
    type: teleport
    params:
      graph: "g1"
"""
    config_file = tmp_path / "job-config.yaml"
    config_file.write_text(yaml_content)

    with pytest.raises(JobConfigValidationError):
        JobConfig.from_yaml_file(config_file)


def test_from_yaml_file_rejects_missing_required_field(tmp_path) -> None:  # type: ignore[no-untyped-def]
    yaml_content = """
projections:
  - cypher:
      graph: "g1"
steps:
  - name: "Run WCC"
    type: algorithm
    params:
      algorithm: "wcc"
      graph: "g1"
      mode: "stream"
"""
    config_file = tmp_path / "job-config.yaml"
    config_file.write_text(yaml_content)

    with pytest.raises(JobConfigValidationError, match="query"):
        JobConfig.from_yaml_file(config_file)


def test_from_yaml_file_rejects_empty_projections(tmp_path) -> None:  # type: ignore[no-untyped-def]
    yaml_content = """
projections: []
steps:
  - name: "Run WCC"
    type: algorithm
    params:
      algorithm: "wcc"
      graph: "g1"
      mode: "stream"
"""
    config_file = tmp_path / "job-config.yaml"
    config_file.write_text(yaml_content)

    with pytest.raises(JobConfigValidationError, match="projections"):
        JobConfig.from_yaml_file(config_file)
