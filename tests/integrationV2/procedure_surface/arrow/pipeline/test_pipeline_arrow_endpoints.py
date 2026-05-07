from uuid import uuid4

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
