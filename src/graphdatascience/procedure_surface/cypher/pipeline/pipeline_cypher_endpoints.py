from graphdatascience.procedure_surface.api.pipeline.pipeline_endpoints import PipelineEndpoints
from graphdatascience.procedure_surface.cypher.pipeline.link_prediction_pipeline_cypher_endpoints import (
    LinkPredictionPipelineCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pipeline.node_classification_pipeline_cypher_endpoints import (
    NodeClassificationPipelineCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pipeline.node_regression_pipeline_cypher_endpoints import (
    NodeRegressionPipelineCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pipeline.pipeline_catalog_cypher_endpoints import (
    PipelineCatalogCypherEndpoints,
)


class PipelineCypherEndpoints(PipelineCatalogCypherEndpoints, PipelineEndpoints):
    @property
    def link_prediction(self) -> LinkPredictionPipelineCypherEndpoints:
        return LinkPredictionPipelineCypherEndpoints(self._query_runner)

    @property
    def node_classification(self) -> NodeClassificationPipelineCypherEndpoints:
        return NodeClassificationPipelineCypherEndpoints(self._query_runner)

    @property
    def node_regression(self) -> NodeRegressionPipelineCypherEndpoints:
        return NodeRegressionPipelineCypherEndpoints(self._query_runner)
