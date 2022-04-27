from ..caller_base import CallerBase
from .graph_proc_runner import GraphProcRunner


class GraphEndpoints(CallerBase):
    @property
    def graph(self) -> GraphProcRunner:
        return GraphProcRunner(self._query_runner, f"{self._namespace}.graph", self._server_version)
