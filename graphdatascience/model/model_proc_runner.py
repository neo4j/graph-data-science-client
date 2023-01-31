from typing import Any, Optional

from pandas import DataFrame, Series

from ..error.client_only_endpoint import client_only_endpoint
from .model import Model
from .model_resolver import ModelResolver


class ModelProcRunner(ModelResolver):
    def list(self, model: Optional[Model] = None) -> DataFrame:
        self._namespace += ".list"

        if model:
            query = f"CALL {self._namespace}($model_name)"
            params = {"model_name": model.name()}
        else:
            query = f"CALL {self._namespace}()"
            params = {}

        return self._query_runner.run_query(query, params)

    def exists(self, model_name: str) -> "Series[Any]":
        self._namespace += ".exists"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model_name}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def drop(self, model: Model) -> "Series[Any]":
        self._namespace += ".drop"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model.name()}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    @client_only_endpoint("gds.model")
    def get(self, model_name: str) -> Model:
        query = "CALL gds.beta.model.list($model_name)"
        params = {"model_name": model_name}
        result = self._query_runner.run_query(query, params)

        if len(result) == 0:
            raise ValueError(f"No loaded model named '{model_name}' exists")

        model_type = result["modelInfo"][0]["modelType"]
        return self._resolve_model(model_type, model_name)
