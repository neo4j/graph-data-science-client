from ..caller_base import CallerBase
from .topological_lp_runner import TopologicalLPRunner


class TopologicalLPEndpoints(CallerBase):
    @property
    def linkprediction(self) -> TopologicalLPRunner:
        return TopologicalLPRunner(self._query_runner, f"{self._namespace}.linkprediction", self._server_version)
