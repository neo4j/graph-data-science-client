from typing import Any

import neo4j

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.pipeline.pipeline_endpoints import PipelineCatalogEntry, PipelineEndpoints
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

    def list(self, pipeline_name: str | None = None) -> list[PipelineCatalogEntry]:
        params = CallParameters()
        if pipeline_name is not None:
            params["pipeline_name"] = pipeline_name
        result = self._query_runner.call_procedure("gds.pipeline.list", params=params, custom_error=False)
        return [self._to_pipeline_catalog_entry(row.to_dict()) for _, row in result.iterrows()]

    @property
    def node_classification(self) -> NodeClassificationPipelineCypherEndpoints:
        return NodeClassificationPipelineCypherEndpoints(self._query_runner)

    @property
    def node_regression(self) -> NodeRegressionPipelineCypherEndpoints:
        return NodeRegressionPipelineCypherEndpoints(self._query_runner)

    def _to_pipeline_catalog_entry(self, result: dict[str, Any]) -> PipelineCatalogEntry:
        creation_time = result.get("creationTime", None)
        if creation_time and isinstance(creation_time, neo4j.time.DateTime):
            result["creationTime"] = creation_time.to_native()
        return PipelineCatalogEntry(**result)
