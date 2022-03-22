from typing import Any, Dict, Type, TypeVar, Union

from neo4j import Driver, GraphDatabase
from pandas.core.frame import DataFrame

from .call_builder import CallBuilder
from .direct_endpoints import DirectEndpoints
from .error.uncallable_namespace import UncallableNamespace
from .query_runner.neo4j_query_runner import Neo4jQueryRunner
from .query_runner.query_runner import QueryRunner
from .version import __version__

GDS = TypeVar("GDS", bound="GraphDataScience")


class GraphDataScience(DirectEndpoints, UncallableNamespace):
    _AURA_DS_PROTOCOL = "neo4j+s"

    def __init__(self, endpoint: Union[str, QueryRunner], auth: Any = None, aura_ds: bool = False):
        if isinstance(endpoint, str):
            self._config: Dict[str, Any] = {"user_agent": f"neo4j-graphdatascience-v{__version__}"}

            if aura_ds:
                protocol = endpoint.split(":")[0]
                if not protocol == self._AURA_DS_PROTOCOL:
                    raise ValueError(
                        f"AuraDS requires using the '{self._AURA_DS_PROTOCOL}' protocol ('{protocol}' was provided)"
                    )

                self._config["max_connection_lifetime"] = 60 * 8  # 8 minutes
                self._config["keep_alive"] = True
                self._config["max_connection_pool_size"] = 50

            driver = GraphDatabase.driver(endpoint, auth=auth, **self._config)

            self._query_runner = self.create_neo4j_query_runner(driver, auto_close=True)
        else:
            self._query_runner = endpoint

        super().__init__(self._query_runner, "gds")

    def __getattr__(self, attr: str) -> CallBuilder:
        return CallBuilder(self._query_runner, f"gds.{attr}")

    def set_database(self, db: str) -> None:
        self._query_runner.set_database(db)

    def run_cypher(self, query: str, params: Dict[str, Any] = {}) -> DataFrame:
        return self._query_runner.run_query(query, params)

    def driver_config(self) -> Dict[str, Any]:
        return self._config

    @classmethod
    def from_neo4j_driver(cls: Type[GDS], driver: Driver) -> "GraphDataScience":
        return cls(cls.create_neo4j_query_runner(driver))

    @staticmethod
    def create_neo4j_query_runner(driver: Driver, auto_close: bool = False) -> Neo4jQueryRunner:
        return Neo4jQueryRunner(driver, auto_close=auto_close)
