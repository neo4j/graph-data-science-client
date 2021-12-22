from .graph import Graph


class GraphProjectRunner:
    def __init__(self, query_runner, namespace):
        self._query_runner = query_runner
        self._namespace = namespace

    def __call__(self, graph_name, node_spec, relationship_spec):
        self._query_runner.run_query(
            f"CALL {self._namespace}($graph_name, $node_spec, $relationship_spec)",
            {
                "graph_name": graph_name,
                "node_spec": node_spec,
                "relationship_spec": relationship_spec,
            },
        )

        return Graph(graph_name)

    def estimate(self, node_spec, relationship_spec):
        self._namespace += ".estimate"
        result = self._query_runner.run_query(
            f"CALL {self._namespace}($node_spec, $relationship_spec)",
            {
                "node_spec": node_spec,
                "relationship_spec": relationship_spec,
            },
        )

        return result

    @property
    def cypher(self):
        return GraphProjectRunner(self._query_runner, self._namespace + ".cypher")

    def subgraph(
        self, graph_name, from_graph, node_filter, relationship_filter, **config
    ):
        self._namespace += ".subgraph"
        self._query_runner.run_query(
            f"CALL {self._namespace}($graph_name, $from_graph_name, $node_filter, $relationship_filter, $config)",
            {
                "graph_name": graph_name,
                "from_graph_name": from_graph.name,
                "node_filter": node_filter,
                "relationship_filter": relationship_filter,
                "config": config,
            },
        )

        return Graph(graph_name)
