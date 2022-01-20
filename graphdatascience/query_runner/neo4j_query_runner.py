from typing import Any, Dict

import neo4j

from .query_runner import QueryResult, QueryRunner


class Neo4jQueryRunner(QueryRunner):
    def __init__(self, driver: neo4j.Driver, db: Any = neo4j.DEFAULT_DATABASE):
        self._driver = driver
        self._db = db

    def run_query(self, query: str, params: Dict[str, Any] = {}) -> QueryResult:
        with self._driver.session(database=self._db) as session:
            result = session.run(query, params)
            return result.data()  # type: ignore

    def set_database(self, db: str) -> None:
        self._db = db
