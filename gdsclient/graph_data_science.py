from typing import Any

from .call_builder import CallBuilder
from .query_runner.query_runner import QueryRunner


class GraphDataScience:
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def __getattr__(self, attr: str) -> Any:
        return getattr(CallBuilder(self._query_runner, "gds"), attr)

    def set_database(self, db: str) -> None:
        self._query_runner.set_database(db)
