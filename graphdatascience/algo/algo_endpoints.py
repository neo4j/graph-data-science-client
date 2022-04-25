from ..caller_base import CallerBase
from .algo_proc_runner import GraphSageRunner, StandardModeRunner, StreamModeRunner


class AlgoEndpoints(CallerBase):
    @property
    def train(self) -> GraphSageRunner:
        return GraphSageRunner(self._query_runner, f"{self._namespace}.train", self._server_version)

    @property
    def stream(self) -> StreamModeRunner:
        return StreamModeRunner(self._query_runner, f"{self._namespace}.stream", self._server_version)

    @property
    def mutate(self) -> StandardModeRunner:
        return StandardModeRunner(self._query_runner, f"{self._namespace}.mutate", self._server_version)

    @property
    def stats(self) -> StandardModeRunner:
        return StandardModeRunner(self._query_runner, f"{self._namespace}.stats", self._server_version)

    @property
    def write(self) -> StandardModeRunner:
        return StandardModeRunner(self._query_runner, f"{self._namespace}.write", self._server_version)
