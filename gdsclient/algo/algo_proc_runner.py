class AlgoProcRunner:
    def __init__(self, query_runner, proc_name):
        self._query_runner = query_runner
        self._proc_name = proc_name

    def _run_procedure(self, graph, config):
        query = f"CALL {self._proc_name}($graph_name, $config)"

        params = {}
        params["graph_name"] = graph.name()
        params["config"] = config

        return self._query_runner.run_query(query, params)

    def __call__(self, graph, **config):
        return self._run_procedure(graph, config)

    def estimate(self, graph, **config):
        self._proc_name += "." + "estimate"
        return self._run_procedure(graph, config)
