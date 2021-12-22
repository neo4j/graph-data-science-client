from .graph import Graph


class GraphProjectRunner:
    def __init__(self, query_runner, proc_name="gds.graph.project"):
        self._query_runner = query_runner
        self._proc_name = proc_name

    def __call__(self, graph_name, node_spec, relationship_spec):
        self._query_runner.run_query(
            f"CALL {self._proc_name}($graph_name, $node_spec, $relationship_spec)",
            {
                "graph_name": graph_name,
                "node_spec": node_spec,
                "relationship_spec": relationship_spec,
            },
        )

        return Graph(graph_name, node_spec, relationship_spec)

    def estimate(self, node_spec, relationship_spec):
        self._proc_name += ".estimate"
        result = self._query_runner.run_query(
            f"CALL {self._proc_name}($node_spec, $relationship_spec)",
            {
                "node_spec": node_spec,
                "relationship_spec": relationship_spec,
            },
        )

        return result

    @property
    def cypher(self):
        return GraphProjectRunner(self._query_runner, self._proc_name + ".cypher")
