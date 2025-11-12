from ..caller_base import CallerBase
from .topological_lp_alpha_runner import TopologicalLPAlphaRunner


class TopologicalLPAlphaEndpoints(CallerBase):
    @property
    def linkprediction(self) -> TopologicalLPAlphaRunner:
        return TopologicalLPAlphaRunner(self._query_runner, f"{self._namespace}.linkprediction", self._server_version)
