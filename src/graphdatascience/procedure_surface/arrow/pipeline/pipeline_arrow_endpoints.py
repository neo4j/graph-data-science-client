from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.pipeline.pipeline_endpoints import PipelineEndpoints
from graphdatascience.procedure_surface.arrow.pipeline.link_prediction_pipeline_arrow_endpoints import (
    LinkPredictionPipelineArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.node_classification_pipeline_arrow_endpoints import (
    NodeClassificationPipelineArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.node_regression_pipeline_arrow_endpoints import (
    NodeRegressionPipelineArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.pipeline_catalog_arrow_endpoints import (
    PipelineCatalogArrowEndpoints,
)
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol


class PipelineArrowEndpoints(PipelineCatalogArrowEndpoints, PipelineEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None,
        show_progress: bool = True,
    ) -> None:
        self._arrow_client = arrow_client
        self._show_progress = show_progress
        self._write_protocol = write_protocol

    @property
    def link_prediction(self) -> LinkPredictionPipelineArrowEndpoints:
        return LinkPredictionPipelineArrowEndpoints(
            self._arrow_client,
            self._write_protocol,
            show_progress=self._show_progress,
        )

    @property
    def node_classification(self) -> NodeClassificationPipelineArrowEndpoints:
        return NodeClassificationPipelineArrowEndpoints(
            self._arrow_client,
            self._write_protocol,
            show_progress=self._show_progress,
        )

    @property
    def node_regression(self) -> NodeRegressionPipelineArrowEndpoints:
        return NodeRegressionPipelineArrowEndpoints(
            self._arrow_client,
            show_progress=self._show_progress,
        )
