from .call_builder import IndirectBetaCallBuilder
from .graph.graph_endpoints import GraphBetaEndpoints
from .model.model_endpoints import ModelBetaEndpoints
from .pipeline.pipeline_endpoints import PipelineBetaEndpoints
from .query_runner.query_runner import QueryRunner
from .server_version.server_version import ServerVersion
from .system.system_endpoints import SystemBetaEndpoints


class BetaEndpoints(GraphBetaEndpoints, PipelineBetaEndpoints, ModelBetaEndpoints, SystemBetaEndpoints):
    def __init__(self, query_runner: QueryRunner, namespace: str, server_version: ServerVersion):
        super().__init__(query_runner, namespace, server_version)

    def __getattr__(self, attr: str) -> IndirectBetaCallBuilder:
        return IndirectBetaCallBuilder(self._query_runner, f"{self._namespace}.{attr}", self._server_version)
