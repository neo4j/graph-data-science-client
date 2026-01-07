from __future__ import annotations

from typing import Any

import neo4j

from graphdatascience.call_parameters import CallParameters
from graphdatascience.model.v2.model_details import ModelDetails
from graphdatascience.procedure_surface.api.model.model_catalog_endpoints import (
    ModelCatalogEndpoints,
    ModelDeleteResult,
    ModelExistsResult,
    ModelLoadResult,
    ModelStoreResult,
)
from graphdatascience.query_runner.query_runner import QueryRunner


class ModelCatalogCypherEndpoints(ModelCatalogEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def list(self) -> list[ModelDetails]:
        df = self._query_runner.call_procedure("gds.model.list", params=CallParameters(), custom_error=False)
        return [self._to_model_details(row.to_dict()) for _, row in df.iterrows()]

    def exists(self, model_name: str) -> ModelExistsResult | None:
        # Use list to also retrieve modelType; if not found, return None
        params = CallParameters(name=model_name)
        df = self._query_runner.call_procedure("gds.model.list", params=params, custom_error=False)
        if df.empty:
            return None
        row = df.iloc[0].to_dict()
        model_type = str(row.get("modelType")) if "modelType" in row else None
        if model_type is None and "modelInfo" in row and isinstance(row["modelInfo"], dict):
            # Older servers might nest model type
            model_type = str(row["modelInfo"].get("modelType"))
        return ModelExistsResult(modelName=model_name, modelType=model_type or "", exists=True)

    def get(self, model_name: str) -> ModelDetails:
        params = CallParameters(name=model_name)
        df = self._query_runner.call_procedure("gds.model.list", params=params, custom_error=False)
        if df.empty:
            raise ValueError(f"Model with name `{model_name}` does not exist")
        return self._to_model_details(df.iloc[0].to_dict())

    def drop(self, model_name: str, *, fail_if_missing: bool = False) -> ModelDetails:
        params = CallParameters(model_name=model_name, fail_if_missing=fail_if_missing)
        df = self._query_runner.call_procedure("gds.model.drop", params=params, custom_error=False)
        if df.empty:
            raise ValueError(f"Model with name `{model_name}` does not exist")
        return self._to_model_details(df.iloc[0].to_dict())

    def delete(self, model_name: str) -> ModelDeleteResult:
        params = CallParameters(model_name=model_name)
        df = self._query_runner.call_procedure("gds.model.delete", params=params, custom_error=False)
        if df.empty:
            raise ValueError(f"Model with name `{model_name}` does not exist")
        return ModelDeleteResult(**df.iloc[0].to_dict())

    def load(self, model_name: str) -> ModelLoadResult:
        params = CallParameters(model_name=model_name)
        df = self._query_runner.call_procedure("gds.model.load", params=params, custom_error=False)
        if df.empty:
            raise ValueError(f"Model with name `{model_name}` does not exist")
        return ModelLoadResult(**df.iloc[0].to_dict())

    def store(self, model_name: str, *, fail_if_unsupported: bool = False) -> ModelStoreResult:
        # Historical parameter name is 'fail_flag' in some versions
        params = CallParameters(model_name=model_name, fail_flag=fail_if_unsupported)
        df = self._query_runner.call_procedure("gds.model.store", params=params, custom_error=False)
        if df.empty:
            raise ValueError(f"Model with name `{model_name}` does not exist")
        return ModelStoreResult(**df.iloc[0].to_dict())

    def _to_model_details(self, result: dict[str, Any]) -> ModelDetails:
        creation_time = result.get("creationTime", None)
        if creation_time and isinstance(creation_time, neo4j.time.DateTime):
            result["creationTime"] = creation_time.to_native()
        return ModelDetails(**result)
