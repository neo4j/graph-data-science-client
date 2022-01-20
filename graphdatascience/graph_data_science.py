from typing import Type, TypeVar

from neo4j import Driver

from .call_builder import CallBuilder
from .direct_endpoints import DirectEndpoints
from .error.uncallable_namespace import UncallableNamespace
from .query_runner.neo4j_query_runner import Neo4jQueryRunner
from .query_runner.query_runner import QueryRunner

GDS = TypeVar("GDS", bound="GraphDataScience")


class GraphDataScience(DirectEndpoints, UncallableNamespace):
    def __init__(self, query_runner: QueryRunner):
        super().__init__(query_runner, "gds")
        self._query_runner = query_runner

    def __getattr__(self, attr: str) -> CallBuilder:
        return CallBuilder(self._query_runner, f"gds.{attr}")

    def set_database(self, db: str) -> None:
        self._query_runner.set_database(db)

    @classmethod
    def from_neo4j_driver(cls: Type[GDS], driver: Driver) -> "GraphDataScience":
        return cls(cls.create_neo4j_query_runner(driver))

    @staticmethod
    def create_neo4j_query_runner(driver: Driver) -> Neo4jQueryRunner:
        return Neo4jQueryRunner(driver)
