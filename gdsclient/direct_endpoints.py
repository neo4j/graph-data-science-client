from .graph.graph_endpoints import GraphEndpoints
from .query_runner.query_runner import QueryRunner


class DirectEndpoints(GraphEndpoints):
    def __init__(self, query_runner: QueryRunner, namespace: str):
        super().__init__(query_runner, namespace)
