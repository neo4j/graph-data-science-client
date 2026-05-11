from uuid import uuid4

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.pipeline.node_regression_pipeline_arrow_endpoints import (
    NodeRegressionPipelineArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.pipeline_arrow_endpoints import PipelineArrowEndpoints


def test_pipeline_list_returns_created_pipeline(arrow_client: AuthenticatedArrowClient) -> None:
    pipeline_name = f"pipeline-list-arrow-{uuid4().hex[:8]}"

    try:
        NodeRegressionPipelineArrowEndpoints(arrow_client, None, show_progress=False).create(pipeline_name)

        pipelines = PipelineArrowEndpoints(arrow_client, None, show_progress=False).list(pipeline_name)

        assert len(pipelines) == 1
        assert pipelines[0].pipeline_name == pipeline_name
        assert pipelines[0].pipeline_type == "Node regression training pipeline"
    finally:
        arrow_client.do_action_with_retry(
            "v2/pipeline.drop",
            {"pipelineName": pipeline_name, "failIfMissing": False},
        )


def test_pipeline_arrow_exists_and_drop_round_trip(arrow_client: AuthenticatedArrowClient) -> None:
    pipeline_name = f"pipeline-exists-arrow-{uuid4().hex[:8]}"
    pipeline_surface = PipelineArrowEndpoints(arrow_client, None, show_progress=False)

    try:
        NodeRegressionPipelineArrowEndpoints(arrow_client, None, show_progress=False).create(pipeline_name)

        exists_result = pipeline_surface.exists(pipeline_name)
        assert exists_result is not None
        assert exists_result.pipeline_name == pipeline_name
        assert exists_result.exists is True

        drop_result = pipeline_surface.drop(pipeline_name)
        assert drop_result is not None
        assert drop_result.pipeline_name == pipeline_name
        assert pipeline_surface.exists(pipeline_name) is None
        assert pipeline_surface.drop(pipeline_name) is None

        with pytest.raises(Exception):
            pipeline_surface.drop(pipeline_name, fail_if_missing=True)
    finally:
        arrow_client.do_action_with_retry(
            "v2/pipeline.drop",
            {"pipelineName": pipeline_name, "failIfMissing": False},
        )
