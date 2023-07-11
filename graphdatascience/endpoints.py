from .algo.single_mode_algo_endpoints import SingleModeAlgoEndpoints
from .call_builder import IndirectAlphaCallBuilder, IndirectBetaCallBuilder
from .gnn.gnn_endpoints import GnnEndpoints
from .graph.graph_endpoints import (
    GraphAlphaEndpoints,
    GraphBetaEndpoints,
    GraphEndpoints,
)
from .model.model_endpoints import (
    ModelAlphaEndpoints,
    ModelBetaEndpoints,
    ModelEndpoints,
)
from .pipeline.pipeline_endpoints import (
    PipelineAlphaEndpoints,
    PipelineBetaEndpoints,
    PipelineEndpoints,
)
from .query_runner.query_runner import QueryRunner
from .server_version.server_version import ServerVersion
from .system.config_endpoints import ConfigEndpoints
from .system.system_endpoints import (
    DirectSystemEndpoints,
    SystemAlphaEndpoints,
    SystemBetaEndpoints,
)
from .topological_lp.topological_lp_endpoints import TopologicalLPAlphaEndpoints
from .utils.util_endpoints import DirectUtilEndpoints

"""
This class should inherit endpoint classes that only contain endpoints that can be called directly from
the `gds` namespace. Example of such endpoints are: "graph" and "list".
"""


class DirectEndpoints(
    DirectSystemEndpoints, DirectUtilEndpoints, GraphEndpoints, PipelineEndpoints, ModelEndpoints, GnnEndpoints
):
    def __init__(self, query_runner: QueryRunner, namespace: str, server_version: ServerVersion):
        super().__init__(query_runner, namespace, server_version)


"""
This class should inherit endpoint classes that only expose calls of the `gds.beta` namespace.
Example of such endpoints are: "gds.alpha.model.store" and "gds.alpha.listProgress".
"""


class AlphaEndpoints(
    GraphAlphaEndpoints,
    PipelineAlphaEndpoints,
    TopologicalLPAlphaEndpoints,
    ModelAlphaEndpoints,
    SingleModeAlgoEndpoints,
    SystemAlphaEndpoints,
    ConfigEndpoints,
):
    def __init__(self, query_runner: QueryRunner, namespace: str, server_version: ServerVersion):
        super().__init__(query_runner, namespace, server_version)

    def __getattr__(self, attr: str) -> IndirectAlphaCallBuilder:
        return IndirectAlphaCallBuilder(self._query_runner, f"{self._namespace}.{attr}", self._server_version)


"""
This class should inherit endpoint classes that only expose calls of the `gds.beta` namespace.
Example of such endpoints are: "gds.beta.model.list" and "gds.beta.listProgress".
"""


class BetaEndpoints(GraphBetaEndpoints, PipelineBetaEndpoints, ModelBetaEndpoints, SystemBetaEndpoints):
    def __init__(self, query_runner: QueryRunner, namespace: str, server_version: ServerVersion):
        super().__init__(query_runner, namespace, server_version)

    def __getattr__(self, attr: str) -> IndirectBetaCallBuilder:
        return IndirectBetaCallBuilder(self._query_runner, f"{self._namespace}.{attr}", self._server_version)
