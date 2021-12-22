from .algo_proc_runner import AlgoProcRunner


class AlgoEndpoints:
    def __init__(self, query_runner, namespace):
        self._query_runner = query_runner
        self._namespace = namespace

    @property
    def mutate(self):
        return AlgoProcRunner(self._query_runner, self._namespace + ".mutate")

    @property
    def stats(self):
        return AlgoProcRunner(self._query_runner, self._namespace + ".stats")

    @property
    def stream(self):
        return AlgoProcRunner(self._query_runner, self._namespace + ".stream")

    @property
    def write(self):
        return AlgoProcRunner(self._query_runner, self._namespace + ".write")
