from graphdatascience.graph.base_graph_proc_runner import BaseGraphProcRunner
from graphdatascience.graph.graph_remote_project_runner import GraphProjectRemoteRunner


class GraphRemoteProcRunner(BaseGraphProcRunner):
    @property
    def project(self) -> GraphProjectRemoteRunner:
        return GraphProjectRemoteRunner(self._query_runner, self._namespace, self._server_version)
