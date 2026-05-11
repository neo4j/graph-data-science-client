from unittest import mock

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.pipeline.pipeline_arrow_endpoints import PipelineArrowEndpoints


def test_pipeline_arrow_list_runs_list_action() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    row = mock.Mock()
    row.body.to_pybytes.return_value = b'{"pipelineName":"pipe","pipelineType":"Node classification training pipeline"}'
    arrow_client.do_action_with_retry.return_value = [row]

    result = PipelineArrowEndpoints(arrow_client, None).list("pipe")

    assert len(result) == 1
    assert result[0].pipeline_name == "pipe"
    arrow_client.do_action_with_retry.assert_called_once_with("v2/pipeline.list", {"pipelineName": "pipe"})


def test_pipeline_arrow_exists_returns_typed_result_when_pipeline_exists() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    row = mock.Mock()
    row.body.to_pybytes.return_value = b'{"pipelineName":"pipe","pipelineType":"Node classification training pipeline"}'
    arrow_client.do_action_with_retry.return_value = [row]

    result = PipelineArrowEndpoints(arrow_client, None).exists("pipe")

    assert result is not None
    assert result.pipeline_name == "pipe"
    assert result.pipeline_type == "Node classification training pipeline"
    assert result.exists is True


def test_pipeline_arrow_exists_returns_none_when_pipeline_is_missing() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = []

    assert PipelineArrowEndpoints(arrow_client, None).exists("missing") is None


def test_pipeline_arrow_drop_returns_catalog_entry_when_pipeline_exists() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    row = mock.Mock()
    row.body.to_pybytes.return_value = b'{"pipelineName":"pipe","pipelineType":"Node classification training pipeline"}'
    arrow_client.do_action_with_retry.return_value = [row]

    result = PipelineArrowEndpoints(arrow_client, None).drop("pipe")

    assert result is not None
    assert result.pipeline_name == "pipe"
    assert result.pipeline_type == "Node classification training pipeline"
    arrow_client.do_action_with_retry.assert_called_once_with(
        "v2/pipeline.drop",
        {"pipelineName": "pipe", "failIfMissing": False},
    )


def test_pipeline_arrow_drop_returns_none_when_pipeline_is_missing_and_flag_is_false() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = []

    assert PipelineArrowEndpoints(arrow_client, None).drop("missing") is None


def test_pipeline_arrow_drop_raises_when_pipeline_is_missing_and_flag_is_true() -> None:
    arrow_client = mock.Mock(spec=AuthenticatedArrowClient)
    arrow_client.do_action_with_retry.return_value = []

    with pytest.raises(ValueError, match="Pipeline with name `missing` does not exist"):
        PipelineArrowEndpoints(arrow_client, None).drop("missing", fail_if_missing=True)
