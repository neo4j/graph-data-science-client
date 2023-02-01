from .algo.algo_endpoints import AlgoEndpoints
from .query_runner.query_runner import QueryRunner
from .server_version.server_version import ServerVersion

"""
This class should inherit endpoint classes that only contain endpoints that needs more of a prefix
than `gds` for some calls. An example of such an endpoint is "graph" which sometimes needs `gds.alpha`
as prefix, eg. for `gds.alpha.graph.construct`.
"""


class IndirectProductEndpoints(AlgoEndpoints):
    def __init__(self, query_runner: QueryRunner, namespace: str, server_version: ServerVersion):
        super().__init__(query_runner, namespace, server_version)
