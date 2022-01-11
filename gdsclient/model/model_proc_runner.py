from typing import Optional, Union

from ..pipeline.lp_prediction_pipeline import LPPredictionPipeline
from ..pipeline.lp_training_pipeline import LPTrainingPipeline
from ..pipeline.nc_prediction_pipeline import NCPredictionPipeline
from ..pipeline.nc_training_pipeline import NCTrainingPipeline
from ..query_runner.query_runner import QueryResult, QueryRunner
from .model import Model
from .trained_model import GraphSageModel

ModelId = Union[Model, str]


class ModelProcRunner:
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    @staticmethod
    def _model_name(model_id: ModelId) -> str:
        if isinstance(model_id, str):
            return model_id
        elif isinstance(model_id, Model):
            return model_id.name()

        raise ValueError(
            f"Provided model identifier is of the wrong type: {type(model_id)}"
        )

    # TODO: Figure out how to integration test
    def store(
        self, model_id: ModelId, failIfUnsupportedType: bool = True
    ) -> QueryResult:
        self._namespace += ".store"

        query = f"CALL {self._namespace}($model_name, $fail_flag)"
        params = {
            "model_name": ModelProcRunner._model_name(model_id),
            "fail_flag": failIfUnsupportedType,
        }

        return self._query_runner.run_query(query, params)

    def list(self, model_id: Optional[ModelId] = None) -> QueryResult:
        self._namespace += ".list"

        if model_id:
            query = f"CALL {self._namespace}($model_name)"
            params = {"model_name": ModelProcRunner._model_name(model_id)}
        else:
            query = f"CALL {self._namespace}()"
            params = {}

        return self._query_runner.run_query(query, params)

    def exists(self, model_id: ModelId) -> QueryResult:
        self._namespace += ".exists"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": ModelProcRunner._model_name(model_id)}

        return self._query_runner.run_query(query, params)

    def publish(self, model_id: ModelId) -> Model:
        self._namespace += ".publish"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": ModelProcRunner._model_name(model_id)}

        result = self._query_runner.run_query(query, params)

        return Model(result[0]["modelInfo"]["modelName"], self._query_runner)

    def drop(self, model_id: ModelId) -> QueryResult:
        self._namespace += ".drop"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": ModelProcRunner._model_name(model_id)}

        return self._query_runner.run_query(query, params)

    # TODO: Figure out how to integration test
    def load(self, model_name: str) -> Model:
        self._namespace += ".load"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": model_name}

        result = self._query_runner.run_query(query, params)

        return Model(result[0]["modelName"], self._query_runner)

    # TODO: Figure out how to integration test
    def delete(self, model_id: ModelId) -> QueryResult:
        self._namespace += ".delete"

        query = f"CALL {self._namespace}($model_name)"
        params = {"model_name": ModelProcRunner._model_name(model_id)}

        return self._query_runner.run_query(query, params)

    def get(self, model_name: str) -> Model:
        if self._namespace != "gds.model":
            raise SyntaxError(f"There is no {self._namespace + '.get'} to call")

        self._namespace = "gds.beta.model"
        result = self.list(model_name)
        if len(result) == 0:
            raise ValueError(f"No loaded model named '{model_name}' exists")

        model_type = result[0]["modelInfo"]["modelType"]
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
