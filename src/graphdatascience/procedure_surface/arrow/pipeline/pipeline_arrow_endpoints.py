from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.pipeline.pipeline_endpoints import PipelineEndpoints
from graphdatascience.procedure_surface.arrow.pipeline.node_classification_pipeline_arrow_endpoints import (
    NodeClassificationPipelineArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.node_regression_pipeline_arrow_endpoints import (
    NodeRegressionPipelineArrowEndpoints,
)


class PipelineArrowEndpoints(PipelineEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None,
        show_progress: bool = True,
    ) -> None:
        self._arrow_client = arrow_client
        self._write_back_client = write_back_client
        self._show_progress = show_progress

    @property
    def node_classification(self) -> NodeClassificationPipelineArrowEndpoints:
        return NodeClassificationPipelineArrowEndpoints(
            self._arrow_client,
            self._write_back_client,
            show_progress=self._show_progress,
        )

    @property
    def node_regression(self) -> NodeRegressionPipelineArrowEndpoints:
        return NodeRegressionPipelineArrowEndpoints(
            self._arrow_client,
            self._write_back_client,
            show_progress=self._show_progress,
        )
