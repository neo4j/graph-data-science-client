from ..query_runner.query_runner import QueryRunner
from .algo_proc_runner import AlgoProcRunner


class AlgoEndpoints:
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    @property
    def mutate(self) -> AlgoProcRunner:
        return AlgoProcRunner(self._query_runner, f"{self._namespace}.mutate")

    @property
    def stats(self) -> AlgoProcRunner:
        return AlgoProcRunner(self._query_runner, f"{self._namespace}.stats")

    @property
    def stream(self) -> AlgoProcRunner:
        return AlgoProcRunner(self._query_runner, f"{self._namespace}.stream")

    @property
    def write(self) -> AlgoProcRunner:
        return AlgoProcRunner(self._query_runner, f"{self._namespace}.write")
