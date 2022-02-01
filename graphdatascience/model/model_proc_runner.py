from typing import Optional

from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..pipeline.lp_prediction_pipeline import LPPredictionPipeline
from ..pipeline.lp_training_pipeline import LPTrainingPipeline
from ..pipeline.nc_prediction_pipeline import NCPredictionPipeline
from ..pipeline.nc_training_pipeline import NCTrainingPipeline
from ..query_runner.query_runner import QueryResult, QueryRunner, Row
from .graphsage_model import GraphSageModel
from .model import Model


class ModelProcRunner(UncallableNamespace, IllegalAttrChecker):
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    def store(self, model: Model, failIfUnsupportedType: bool = True) -> Row:
        self._namespace += ".store"

        query = f"CALL {self._namespace}($model_name, $fail_flag)"
        params = {
            "model_name": model.name(),
            "fail_flag": failIfUnsupportedType,
        }

        return self._query_runner.run_query(query, params)[0]

    def list(self, model: Optional[Model] = None) -> QueryResult:
        self._namespace += ".list"

        if model:
            query = f"CALL {self._namespace}($model_name)"
            params = {"model_name": model.name()}
        else:
            query = f"CALL {self._namespace}()"
            params = {}

        return self._query_runner.run_query(query, params)

    def exists(self, model_name: str) -> Row:
        self._namespace += ".exists"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model_name}

        return self._query_runner.run_query(query, params)[0]

    def publish(self, model: Model) -> Model:
        self._namespace += ".publish"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model.name()}

        result = self._query_runner.run_query(query, params)

        model_name = result[0]["modelInfo"]["modelName"]
        model_type = result[0]["modelInfo"]["modelType"]
        return self._resolve_model(model_type, model_name)

    def drop(self, model: Model) -> Row:
        self._namespace += ".drop"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model.name()}

        return self._query_runner.run_query(query, params)[0]

    def load(self, model_name: str) -> Model:
        self._namespace += ".load"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model_name}

        result = self._query_runner.run_query(query, params)

        self._namespace = "gds.model"
        return self.get(result[0]["modelName"])

    def delete(self, model: Model) -> Row:
        self._namespace += ".delete"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model.name()}

        return self._query_runner.run_query(query, params)[0]

    @client_only_endpoint("gds.model")
    def get(self, model_name: str) -> Model:
        query = "CALL gds.beta.model.list($model_name)"
        params = {"model_name": model_name}
        result = self._query_runner.run_query(query, params)

        if len(result) == 0:
            raise ValueError(f"No loaded model named '{model_name}' exists")

        model_type = result[0]["modelInfo"]["modelType"]
        return self._resolve_model(model_type, model_name)

    def _resolve_model(self, model_type: str, model_name: str) -> Model:
        if model_type == "Link prediction training pipeline":
            return LPTrainingPipeline(model_name, self._query_runner)
        elif model_type == "Node classification training pipeline":
            return NCTrainingPipeline(model_name, self._query_runner)
        elif model_type == "Node classification pipeline":
            return NCPredictionPipeline(model_name, self._query_runner)
        elif model_type == "Link prediction pipeline":
            return LPPredictionPipeline(model_name, self._query_runner)
        elif model_type == "graphSage":
            return GraphSageModel(model_name, self._query_runner)

        raise ValueError(f"Unknown model type encountered: '{model_type}'")
