from typing import Any

import neo4j

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.pipeline.pipeline_endpoints import (
    PipelineCatalogEntry,
    PipelineExistsResult,
)
from graphdatascience.query_runner.query_runner import QueryRunner


class PipelineCatalogCypherEndpoints:
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def list(self, pipeline_name: str | None = None) -> list[PipelineCatalogEntry]:
        params = CallParameters()
        if pipeline_name is not None:
            params["pipeline_name"] = pipeline_name
        result = self._query_runner.call_procedure("gds.pipeline.list", params=params, custom_error=False)
        return [self._to_pipeline_catalog_entry(row.to_dict()) for _, row in result.iterrows()]

    def exists(self, pipeline_name: str) -> PipelineExistsResult | None:
        params = CallParameters(pipeline_name=pipeline_name)
        result = self._query_runner.call_procedure("gds.pipeline.list", params=params, custom_error=False)
        if result.empty:
            return None

        row = result.iloc[0].to_dict()
        return PipelineExistsResult(
            pipelineName=str(row["pipelineName"]),
            pipelineType=str(row["pipelineType"]),
            exists=True,
        )

    def drop(self, pipeline_name: str, *, fail_if_missing: bool = False) -> PipelineCatalogEntry | None:
        params = CallParameters(pipeline_name=pipeline_name, fail_if_missing=fail_if_missing)
        result = self._query_runner.call_procedure("gds.pipeline.drop", params=params, custom_error=False)

        if result.empty and fail_if_missing:
            raise ValueError(f"Pipeline with name `{pipeline_name}` does not exist")
        if result.empty:
            return None

        return self._to_pipeline_catalog_entry(result.iloc[0].to_dict())

    def _to_pipeline_catalog_entry(self, result: dict[str, Any]) -> PipelineCatalogEntry:
        creation_time = result.get("creationTime", None)
        if creation_time and isinstance(creation_time, neo4j.time.DateTime):
            result["creationTime"] = creation_time.to_native()
        return PipelineCatalogEntry(**result)
