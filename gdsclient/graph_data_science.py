from .call_builder import CallBuilder


class GraphDataScience:
    def __init__(self, query_runner):
        self._query_runner = query_runner

    def __getattr__(self, attr):
        return getattr(CallBuilder(self._query_runner, "gds"), attr)

    def set_database(self, db):
        self._query_runner.set_database(db)
