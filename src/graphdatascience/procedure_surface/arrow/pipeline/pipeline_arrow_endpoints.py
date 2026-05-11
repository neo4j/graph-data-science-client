from graphdatascience.procedure_surface.api.pipeline.pipeline_endpoints import PipelineEndpoints
from graphdatascience.procedure_surface.arrow.pipeline.node_classification_pipeline_arrow_endpoints import (
    NodeClassificationPipelineArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.node_regression_pipeline_arrow_endpoints import (
    NodeRegressionPipelineArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.pipeline.pipeline_catalog_arrow_endpoints import (
    PipelineCatalogArrowEndpoints,
)


class PipelineArrowEndpoints(PipelineCatalogArrowEndpoints, PipelineEndpoints):
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
