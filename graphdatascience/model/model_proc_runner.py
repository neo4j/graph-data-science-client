from typing import Any, Optional, Tuple

from pandas import DataFrame, Series

from ..error.client_only_endpoint import client_only_endpoint
from ..server_version.server_version import ServerVersion
from .model import Model
from .model_resolver import ModelResolver
from graphdatascience.server_version.compatible_with import compatible_with


class ModelProcRunner(ModelResolver):
    @client_only_endpoint("gds.model")
    def get(self, model_name: str) -> Model:
        if self._server_version < ServerVersion(2, 5, 0):
            query = "CALL gds.beta.model.list($model_name) YIELD modelInfo RETURN modelInfo.modelType AS modelType"
        else:
            query = "CALL gds.model.list($model_name) YIELD modelType"

        params = {"model_name": model_name}
        result = self._query_runner.run_query(query, params, custom_error=False)

        if len(result) == 0:
            raise ValueError(f"No loaded model named '{model_name}' exists")

        model_type = str(result["modelType"].squeeze())
        return self._resolve_model(model_type, model_name)

    @compatible_with("store", min_inclusive=ServerVersion(2, 5, 0))
    def store(self, model: Model, failIfUnsupportedType: bool = True) -> "Series[Any]":
        self._namespace += ".store"

        query = f"CALL {self._namespace}($model_name, $fail_flag)"
        params = {
            "model_name": model.name(),
            "fail_flag": failIfUnsupportedType,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    @compatible_with("publish", min_inclusive=ServerVersion(2, 5, 0))
    def publish(self, model: Model) -> Model:
        self._namespace += ".publish"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model.name()}

        result = self._query_runner.run_query(query, params)

        model_name = result["modelInfo"][0]["modelName"]
        model_type = result["modelInfo"][0]["modelType"]

        return self._resolve_model(model_type, model_name)

    @compatible_with("load", min_inclusive=ServerVersion(2, 5, 0))
    def load(self, model_name: str) -> Tuple[Model, "Series[Any]"]:
        self._namespace += ".load"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model_name}

        result = self._query_runner.run_query(query, params).squeeze()

        self._namespace = "gds.model"
        proc_runner = ModelProcRunner(self._query_runner, self._namespace, self._server_version)

        return proc_runner.get(result["modelName"]), result

    @compatible_with("delete", min_inclusive=ServerVersion(2, 5, 0))
    def delete(self, model: Model) -> "Series[Any]":
        self._namespace += ".delete"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model.name()}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    @compatible_with("list", min_inclusive=ServerVersion(2, 5, 0))
    def list(self, model: Optional[Model] = None) -> DataFrame:
        self._namespace += ".list"

        if model:
            query = f"CALL {self._namespace}($model_name)"
            params = {"model_name": model.name()}
        else:
            query = f"CALL {self._namespace}()"
            params = {}

        return self._query_runner.run_query(query, params)

    @compatible_with("exists", min_inclusive=ServerVersion(2, 5, 0))
    def exists(self, model_name: str) -> "Series[Any]":
        self._namespace += ".exists"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model_name}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    @compatible_with("drop", min_inclusive=ServerVersion(2, 5, 0))
    def drop(self, model: Model) -> "Series[Any]":
        self._namespace += ".drop"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model.name()}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore
