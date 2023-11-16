from typing import Any, Tuple

from pandas import Series

from .model import Model
from .model_proc_runner import ModelProcRunner
from .model_resolver import ModelResolver


class ModelAlphaProcRunner(ModelResolver):
    def store(self, model: Model, failIfUnsupportedType: bool = True) -> "Series[Any]":
        self._namespace += ".store"
        params = {
            "model_name": model.name(),
            "fail_flag": failIfUnsupportedType,
        }

        return self._query_runner.call_procedure(  # type: ignore
            endpoint=self._namespace,
            body="$model_name, $fail_flag",
            params=params,
        ).squeeze()

    def publish(self, model: Model) -> Model:
        self._namespace += ".publish"
        params = {"model_name": model.name()}

        result = self._query_runner.call_procedure(endpoint=self._namespace, body="$model_name", params=params)

        model_name = result["modelInfo"][0]["modelName"]
        model_type = result["modelInfo"][0]["modelType"]

        return self._resolve_model(model_type, model_name)

    def load(self, model_name: str) -> Tuple[Model, "Series[Any]"]:
        self._namespace += ".load"
        params = {"model_name": model_name}

        result = self._query_runner.call_procedure(
            endpoint=self._namespace, body="$model_name", params=params
        ).squeeze()

        self._namespace = "gds.model"
        proc_runner = ModelProcRunner(self._query_runner, self._namespace, self._server_version)

        return proc_runner.get(result["modelName"]), result

    def delete(self, model: Model) -> "Series[Any]":
        self._namespace += ".delete"
        return self._query_runner.call_procedure(  # type: ignore
            endpoint=self._namespace, body="$model_name", params={"model_name": model.name()}
        ).squeeze()
