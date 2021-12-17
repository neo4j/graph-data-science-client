class AlgoProcRunner:
    def __init__(self, query_runner, proc_name):
        self.query_runner = query_runner
        self.proc_name = proc_name

    def __call__(self, graph, **config):
        query = f"CALL {self.proc_name}($graph_name, $config)"

        params = {}
        params["graph_name"] = graph.name
        params["config"] = config

        return self.query_runner.run_query(query, params)
