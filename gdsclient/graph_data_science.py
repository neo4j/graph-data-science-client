from .call_builder import CallBuilder


class GraphDataScience:
    def __init__(self, query_runner):
        self.query_runner = query_runner

    def __getattr__(self, attr):
        return getattr(CallBuilder(self.query_runner, "gds"), attr)
