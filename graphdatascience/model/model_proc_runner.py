from typing import Optional, Tuple

from pandas.core.frame import DataFrame
from pandas.core.series import Series

from ..caller_base import CallerBase
from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..model.link_prediction_model import LPModel
from ..model.node_classification_model import NCModel
from .graphsage_model import GraphSageModel
from .model import Model


class ModelProcRunner(CallerBase, UncallableNamespace, IllegalAttrChecker):
    def store(self, model: Model, failIfUnsupportedType: bool = True) -> Series:
        self._namespace += ".store"

        query = f"CALL {self._namespace}($model_name, $fail_flag)"
        params = {
            "model_name": model.name(),
            "fail_flag": failIfUnsupportedType,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def list(self, model: Optional[Model] = None) -> DataFrame:
        self._namespace += ".list"

        if model:
            query = f"CALL {self._namespace}($model_name)"
            params = {"model_name": model.name()}
        else:
            query = f"CALL {self._namespace}()"
            params = {}

        return self._query_runner.run_query(query, params)

    def exists(self, model_name: str) -> Series:
        self._namespace += ".exists"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model_name}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def publish(self, model: Model) -> Model:
        self._namespace += ".publish"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model.name()}

        result = self._query_runner.run_query(query, params)

        model_name = result["modelInfo"][0]["modelName"]
        model_type = result["modelInfo"][0]["modelType"]

        return self._resolve_model(model_type, model_name)

    def drop(self, model: Model) -> Series:
        self._namespace += ".drop"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model.name()}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def load(self, model_name: str) -> Tuple[Model, Series]:
        self._namespace += ".load"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model_name}

        result = self._query_runner.run_query(query, params).squeeze()

        self._namespace = "gds.model"
        return self.get(result["modelName"]), result

    def delete(self, model: Model) -> Series:
        self._namespace += ".delete"

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

    def _resolve_model(self, model_type: str, model_name: str) -> Model:
        if model_type == "NodeClassification":
            return NCModel(model_name, self._query_runner, self._server_version)
        elif model_type == "LinkPrediction":
            return LPModel(model_name, self._query_runner, self._server_version)
        elif model_type == "graphSage":
            return GraphSageModel(model_name, self._query_runner, self._server_version)

        raise ValueError(f"Unknown model type encountered: '{model_type}'")
