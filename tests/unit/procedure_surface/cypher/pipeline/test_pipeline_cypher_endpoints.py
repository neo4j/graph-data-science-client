from unittest import mock

import pandas as pd
import pytest

from graphdatascience.procedure_surface.cypher.pipeline.pipeline_cypher_endpoints import PipelineCypherEndpoints


def test_pipeline_cypher_list_runs_query() -> None:
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = pd.DataFrame(
        [{"pipelineName": "pipe", "pipelineType": "Node classification training pipeline"}]
    )

    result = PipelineCypherEndpoints(query_runner).list("pipe")

    assert len(result) == 1
    assert result[0].pipeline_name == "pipe"
    query_runner.call_procedure.assert_called_once_with(
        "gds.pipeline.list",
        params=mock.ANY,
        custom_error=False,
    )
    assert query_runner.call_procedure.call_args.kwargs["params"] == {"pipeline_name": "pipe"}


def test_pipeline_cypher_exists_returns_typed_result_when_pipeline_exists() -> None:
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = pd.DataFrame(
        [{"pipelineName": "pipe", "pipelineType": "Node classification training pipeline"}]
    )

    result = PipelineCypherEndpoints(query_runner).exists("pipe")

    assert result is not None
    assert result.pipeline_name == "pipe"
    assert result.pipeline_type == "Node classification training pipeline"
    assert result.exists is True


def test_pipeline_cypher_exists_returns_none_when_pipeline_is_missing() -> None:
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = pd.DataFrame([])

    assert PipelineCypherEndpoints(query_runner).exists("missing") is None


def test_pipeline_cypher_drop_returns_catalog_entry_when_pipeline_exists() -> None:
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = pd.DataFrame(
        [{"pipelineName": "pipe", "pipelineType": "Node classification training pipeline"}]
    )

    result = PipelineCypherEndpoints(query_runner).drop("pipe")

    assert result is not None
    assert result.pipeline_name == "pipe"
    assert result.pipeline_type == "Node classification training pipeline"
    query_runner.call_procedure.assert_called_once_with(
        "gds.pipeline.drop",
        params=mock.ANY,
        custom_error=False,
    )
    assert query_runner.call_procedure.call_args.kwargs["params"] == {
        "pipeline_name": "pipe",
        "fail_if_missing": False,
    }


def test_pipeline_cypher_drop_returns_none_when_pipeline_is_missing_and_flag_is_false() -> None:
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = pd.DataFrame([])

    assert PipelineCypherEndpoints(query_runner).drop("missing") is None


def test_pipeline_cypher_drop_raises_when_pipeline_is_missing_and_flag_is_true() -> None:
    query_runner = mock.Mock()
    query_runner.call_procedure.return_value = pd.DataFrame([])

    with pytest.raises(ValueError, match="Pipeline with name `missing` does not exist"):
        PipelineCypherEndpoints(query_runner).drop("missing", fail_if_missing=True)
