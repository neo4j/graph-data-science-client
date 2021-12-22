from .algo_proc_runner import AlgoProcRunner


class AlgoEndpoints:
    def __init__(self, query_runner, namespace):
        self.query_runner = query_runner
        self.namespace = namespace

    @property
    def mutate(self):
        return AlgoProcRunner(self.query_runner, self.namespace + ".mutate")

    @property
    def stats(self):
        return AlgoProcRunner(self.query_runner, self.namespace + ".stats")

    @property
    def stream(self):
        return AlgoProcRunner(self.query_runner, self.namespace + ".stream")

    @property
    def write(self):
        return AlgoProcRunner(self.query_runner, self.namespace + ".write")
