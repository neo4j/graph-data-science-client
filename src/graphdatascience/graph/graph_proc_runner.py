from .base_graph_proc_runner import BaseGraphProcRunner
from .graph_cypher_runner import GraphCypherRunner
from .graph_project_runner import GraphProjectRunner


class GraphProcRunner(BaseGraphProcRunner):
    @property
    def project(self) -> GraphProjectRunner:
        self._namespace += ".project"
        return GraphProjectRunner(self._query_runner, self._namespace, self._server_version)

    @property
    def cypher(self) -> GraphCypherRunner:
        self._namespace += ".project"
        return GraphCypherRunner(self._query_runner, self._namespace, self._server_version)
