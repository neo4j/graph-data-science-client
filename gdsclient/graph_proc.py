class GraphProc:
    def __init__(self, query_runner):
        self.query_runner = query_runner

    def create(self, graph_name, node_projection, relationship_projection):
        self.query_runner.run_query(
            "CALL gds.graph.create($graph_name, $node_projection, $relationship_projection)",
            {
                "graph_name": graph_name,
                "node_projection": node_projection,
                "relationship_projection": relationship_projection,
            },
        )
        return Graph(graph_name, node_projection, relationship_projection)


class Graph:
    def __init__(self, name, node_projection, relationship_projection):
        self.name = name
        self.node_projection = node_projection
        self.relationship_projection = relationship_projection
