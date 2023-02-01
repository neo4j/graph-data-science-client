from ..caller_base import CallerBase
from .graph_alpha_proc_runner import GraphAlphaProcRunner
from .graph_beta_proc_runner import GraphBetaProcRunner
from .graph_proc_runner import GraphProcRunner


class GraphEndpoints(CallerBase):
    @property
    def graph(self) -> GraphProcRunner:
        return GraphProcRunner(self._query_runner, f"{self._namespace}.graph", self._server_version)


class GraphAlphaEndpoints(CallerBase):
    @property
    def graph(self) -> GraphAlphaProcRunner:
        return GraphAlphaProcRunner(self._query_runner, f"{self._namespace}.graph", self._server_version)


class GraphBetaEndpoints(CallerBase):
    @property
    def graph(self) -> GraphBetaProcRunner:
        return GraphBetaProcRunner(self._query_runner, f"{self._namespace}.graph", self._server_version)
