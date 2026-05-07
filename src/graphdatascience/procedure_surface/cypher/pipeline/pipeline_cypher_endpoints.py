from graphdatascience.procedure_surface.api.pipeline.pipeline_endpoints import PipelineEndpoints
from graphdatascience.procedure_surface.cypher.pipeline.node_classification_pipeline_cypher_endpoints import (
    NodeClassificationPipelineCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pipeline.node_regression_pipeline_cypher_endpoints import (
    NodeRegressionPipelineCypherEndpoints,
)
from graphdatascience.query_runner.query_runner import QueryRunner


class PipelineCypherEndpoints(PipelineEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    @property
    def node_classification(self) -> NodeClassificationPipelineCypherEndpoints:
        return NodeClassificationPipelineCypherEndpoints(self._query_runner)

    @property
    def node_regression(self) -> NodeRegressionPipelineCypherEndpoints:
        return NodeRegressionPipelineCypherEndpoints(self._query_runner)
