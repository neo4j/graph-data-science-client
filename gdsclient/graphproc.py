from abc import ABC, abstractmethod


class GDS:
    def __init__(self, query_runner):
        self.graph_proc = GraphProc(query_runner)

    @property
    def graph(self):
        return self.graph_proc


class GraphProc:
    def __init__(self, query_runner):
        self.query_runner = query_runner

    def create(self, graph_name, node_projection, relationship_projection):
        self.query_runner.run_query(
            "CALL gds.graph.create($graph_name, $node_projection, $relationship_projection)",
            {"graph_name": "g", "node_projection": "A", "relationship_projection": "R"},
        )
        return Graph(graph_name, node_projection, relationship_projection)


class Graph:
    def __init__(self, graph_name, node_projection, relationship_projection):
        self.graph_name = graph_name
        self.node_projection = node_projection
        self.relationship_projection = relationship_projection


class QueryRunner(ABC):
    @abstractmethod
    def run_query(self, query, query_params):
        pass
