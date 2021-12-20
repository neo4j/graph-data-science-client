class GraphProcBuilder:
    def __init__(self, query_runner):
        self.query_runner = query_runner

    @property
    def create(self):
        return GraphCreateRunner(self.query_runner)


class GraphCreateRunner:
    def __init__(self, query_runner, proc_name="gds.graph.create"):
        self.query_runner = query_runner
        self.proc_name = proc_name

    def __call__(self, graph_name, node_spec, relationship_spec):
        self.query_runner.run_query(
            f"CALL {self.proc_name}($graph_name, $node_spec, $relationship_spec)",
            {
                "graph_name": graph_name,
                "node_spec": node_spec,
                "relationship_spec": relationship_spec,
            },
        )

        return Graph(graph_name, node_spec, relationship_spec)

    def estimate(self, node_spec, relationship_spec):
        self.proc_name += ".estimate"
        result = self.query_runner.run_query(
            f"CALL {self.proc_name}($node_spec, $relationship_spec)",
            {
                "node_spec": node_spec,
                "relationship_spec": relationship_spec,
            },
        )

        return result

    @property
    def cypher(self):
        return GraphCreateRunner(self.query_runner, self.proc_name + ".cypher")


class Graph:
    def __init__(self, name, node_spec, relationship_spec):
        self.name = name
        self.node_spec = node_spec
        self.relationship_spec = relationship_spec
