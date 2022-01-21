from typing import Any, Dict, Type, TypeVar, Union

from neo4j import Driver, GraphDatabase

from .call_builder import CallBuilder
from .direct_endpoints import DirectEndpoints
from .error.uncallable_namespace import UncallableNamespace
from .query_runner.neo4j_query_runner import Neo4jQueryRunner
from .query_runner.query_runner import QueryResult, QueryRunner

GDS = TypeVar("GDS", bound="GraphDataScience")


class GraphDataScience(DirectEndpoints, UncallableNamespace):
    def __init__(
        self, endpoint: Union[str, QueryRunner], auth: Any = None, **config: Any
    ):
        if isinstance(endpoint, str):
            driver = GraphDatabase.driver(endpoint, auth=auth, **config)
            self._query_runner = self.create_neo4j_query_runner(driver)
        else:
            self._query_runner = endpoint

        super().__init__(self._query_runner, "gds")

    def __getattr__(self, attr: str) -> CallBuilder:
        return CallBuilder(self._query_runner, f"gds.{attr}")

    def set_database(self, db: str) -> None:
        self._query_runner.set_database(db)

    def run_cypher(self, query: str, params: Dict[str, Any] = {}) -> QueryResult:
        return self._query_runner.run_query(query, params)

    @classmethod
    def from_neo4j_driver(cls: Type[GDS], driver: Driver) -> "GraphDataScience":
        return cls(cls.create_neo4j_query_runner(driver))

    @staticmethod
    def create_neo4j_query_runner(driver: Driver) -> Neo4jQueryRunner:
        return Neo4jQueryRunner(driver)
