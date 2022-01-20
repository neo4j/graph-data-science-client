from ..query_runner.query_runner import QueryRunner
from .model_proc_runner import ModelProcRunner


class ModelEndpoints:
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    @property
    def model(self) -> ModelProcRunner:
        return ModelProcRunner(self._query_runner, f"{self._namespace}.model")
