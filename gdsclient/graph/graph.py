class Graph:
    def __init__(self, name, query_runner):
        self._name = name
        self._query_runner = query_runner

    def name(self):
        return self._name

    def _graph_info(self):
        return self._query_runner.run_query(
            "CALL gds.graph.list($graph_name)", {"graph_name": self._name}
        )[0]

    def node_count(self):
        return self._graph_info()["nodeCount"]
