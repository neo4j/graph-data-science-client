from .algo.algo_endpoints import AlgoEndpoints
from .algo.single_mode_algo_endpoints import SingleModeAlgoEndpoints
from .graph.graph_endpoints import GraphEndpoints
from .model.model_endpoints import ModelEndpoints
from .pipeline.pipeline_endpoints import PipelineEndpoints
from .query_runner.query_runner import QueryRunner
from .server_version.server_version import ServerVersion
from .system.system_endpoints import IndirectSystemEndpoints
from .topological_lp.topological_lp_endpoints import TopologicalLPEndpoints
from .utils.util_endpoints import IndirectUtilEndpoints
from graphdatascience.system.config_endpoints import IndirectConfigEndpoints

"""
This class should inherit endpoint classes that only contain endpoints that needs more of a prefix
than `gds` for some calls. An example of such an endpoint is "graph" which sometimes needs `gds.alpha`
as prefix, eg. for `gds.alpha.graph.construct`.
"""


class IndirectEndpoints(
    AlgoEndpoints,
    SingleModeAlgoEndpoints,
    GraphEndpoints,
    ModelEndpoints,
    PipelineEndpoints,
    IndirectSystemEndpoints,
    IndirectConfigEndpoints,
    TopologicalLPEndpoints,
    IndirectUtilEndpoints,
):
    def __init__(self, query_runner: QueryRunner, namespace: str, server_version: ServerVersion):
        super().__init__(query_runner, namespace, server_version)
