from .algo_proc_runner import AlgoProcRunner


class AlgoProcBuilder:
    def __init__(self, query_runner, namespace="gds"):
        self.query_runner = query_runner
        self.namespace = namespace

    @property
    def mutate(self):
        return AlgoProcRunner(self.query_runner, self.namespace + "." + "mutate")

    def __getattr__(self, attr):
        namespace = ".".join([self.namespace, attr])
        return AlgoProcBuilder(self.query_runner, namespace)
