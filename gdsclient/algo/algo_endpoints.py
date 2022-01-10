from .algo_proc_runner import (StandardModeRunner,
                               TrainProcRunner)
from ..query_runner.query_runner import QueryRunner


class AlgoEndpoints:
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    @property
    def train(self) -> TrainProcRunner:
        return TrainProcRunner(self._query_runner, f"{self._namespace}.train")

    @property
    def mutate(self) -> StandardModeRunner:
        return StandardModeRunner(self._query_runner, f"{self._namespace}.mutate")

    @property
    def stats(self) -> StandardModeRunner:
        return StandardModeRunner(self._query_runner, f"{self._namespace}.stats")

    @property
    def stream(self) -> StandardModeRunner:
        return StandardModeRunner(self._query_runner, f"{self._namespace}.stream")

    @property
    def write(self) -> StandardModeRunner:
        return StandardModeRunner(self._query_runner, f"{self._namespace}.write")
