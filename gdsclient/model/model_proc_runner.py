from typing import Union

from ..query_runner.query_runner import QueryResult, QueryRunner
from .model import Model

ModelId = Union[Model, str]


class ModelProcRunner:
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    def _model_name(model_id: ModelId) -> str:
        if isinstance(model_id, str):
            return model_id
        elif isinstance(model_id, Model):
            return model_id.name()

        raise ValueError(
            f"Provided model identifier is of the wrong type: {type(model_id)}"
        )

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

    # store, load, delete, publish

    # exists?
