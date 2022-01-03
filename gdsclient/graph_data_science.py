from .call_builder import CallBuilder
from .direct_endpoints import DirectEndpoints
from .query_runner.query_runner import QueryRunner


class GraphDataScience(DirectEndpoints):
    def __init__(self, query_runner: QueryRunner):
        super().__init__(query_runner, "gds")
        self._query_runner = query_runner

    def __getattr__(self, attr: str) -> CallBuilder:
        return CallBuilder(self._query_runner, f"gds.{attr}")

    def set_database(self, db: str) -> None:
        self._query_runner.set_database(db)
