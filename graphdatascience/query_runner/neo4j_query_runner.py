import warnings
from typing import Any, Dict, Optional

import neo4j
from pandas.core.frame import DataFrame

from ..error.unable_to_connect import UnableToConnectError
from .graph_constructor import GraphConstructor
from .query_runner import QueryRunner


class Neo4jQueryRunner(QueryRunner):
    def __init__(self, driver: neo4j.Driver, db: Optional[str] = neo4j.DEFAULT_DATABASE, auto_close: bool = False):
        self._driver = driver
        self._auto_close = auto_close

        if db:
            self._db = db
            return

        try:
            with self._driver.session() as session:
                result = session.run("SHOW DATABASES YIELD name, default WHERE default = TRUE RETURN name")
                self._db = result.data()[0]["name"]
        except Exception as e:
            raise UnableToConnectError(e)

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

    def close(self) -> None:
        self._driver.close()

    def database(self) -> str:
        return self._db

    def __del__(self) -> None:
        if self._auto_close:
            self._driver.close()

    def create_graph_constructor(self, _: str, __: int) -> GraphConstructor:
        raise ValueError("This feature requires the GDS Flight server to be enabled.")
