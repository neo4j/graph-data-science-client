from ..query_runner.query_runner import QueryRunner
from .algo_proc_runner import GraphSageRunner, StandardModeRunner, StreamModeRunner


class AlgoEndpoints:
    def __init__(self, query_runner: QueryRunner, namespace: str):
        self._query_runner = query_runner
        self._namespace = namespace

    @property
    def train(self) -> GraphSageRunner:
        return GraphSageRunner(self._query_runner, f"{self._namespace}.train")

    @property
    def stream(self) -> StreamModeRunner:
        return StreamModeRunner(self._query_runner, f"{self._namespace}.stream")

    @property
    def mutate(self) -> StandardModeRunner:
        return StandardModeRunner(self._query_runner, f"{self._namespace}.mutate")

    @property
    def stats(self) -> StandardModeRunner:
        return StandardModeRunner(self._query_runner, f"{self._namespace}.stats")

    @property
    def write(self) -> StandardModeRunner:
        return StandardModeRunner(self._query_runner, f"{self._namespace}.write")
