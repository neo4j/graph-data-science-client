from gdsclient.algo.algo_endpoints import AlgoEndpoints
from gdsclient.query_runner.query_runner import QueryRunner

from .graph.graph_endpoints import GraphEndpoints


class IndirectEndpoints(AlgoEndpoints, GraphEndpoints):
    def __init__(self, query_runner: QueryRunner, namespace: str):
        super().__init__(query_runner, namespace)
