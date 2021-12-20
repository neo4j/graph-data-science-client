class GraphProcBuilder:
    def __init__(self, query_runner):
        self.query_runner = query_runner

    @property
    def create(self):
        return GraphCreateRunner(self.query_runner)


class GraphCreateRunner:
    def __init__(self, query_runner):
        self.query_runner = query_runner

    def __call__(self, graph_name, node_projection, relationship_projection):
        self.query_runner.run_query(
            "CALL gds.graph.create($graph_name, $node_projection, $relationship_projection)",
            {
                "graph_name": graph_name,
                "node_projection": node_projection,
                "relationship_projection": relationship_projection,
            },
        )

        return Graph(graph_name, node_projection, relationship_projection)

    def estimate(self, node_projection, relationship_projection):
        result = self.query_runner.run_query(
            "CALL gds.graph.create.estimate($node_projection, $relationship_projection)",
            {
                "node_projection": node_projection,
                "relationship_projection": relationship_projection,
            },
        )

        return result


class Graph:
    def __init__(self, name, node_projection, relationship_projection):
        self.name = name
        self.node_projection = node_projection
        self.relationship_projection = relationship_projection
