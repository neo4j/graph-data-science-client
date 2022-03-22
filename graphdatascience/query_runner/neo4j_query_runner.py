import warnings
from typing import Any, Dict

import neo4j
from pandas.core.frame import DataFrame

from .query_runner import QueryRunner


class Neo4jQueryRunner(QueryRunner):
    def __init__(self, driver: neo4j.Driver, db: Any = neo4j.DEFAULT_DATABASE, auto_close: bool = False):
        self._driver = driver
        self._db = db
        self._auto_close = auto_close

    def run_query(self, query: str, params: Dict[str, Any] = {}) -> DataFrame:
        with self._driver.session(database=self._db) as session:
            result = session.run(query, params)

            # Though pandas support may be experimental in the `neo4j` package, it should always
            # be supported in the `graphdatascience` package.
            warnings.filterwarnings(
                "ignore",
                message=r"^pandas support is experimental and might be changed or removed in future versions$",
            )

            return result.to_df()  # type: ignore

    def set_database(self, db: str) -> None:
        self._db = db

    def __del__(self) -> None:
        if self._auto_close:
            self._driver.close()
