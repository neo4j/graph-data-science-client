import re
from typing import Any, Dict, Type, TypeVar, Union

from neo4j import Driver, GraphDatabase
from pandas import Series
from pandas.core.frame import DataFrame

from .call_builder import CallBuilder
from .direct_endpoints import DirectEndpoints
from .error.uncallable_namespace import UncallableNamespace
from .query_runner.arrow_query_runner import ArrowQueryRunner
from .query_runner.neo4j_query_runner import Neo4jQueryRunner
from .query_runner.query_runner import QueryRunner
from .server_version.server_version import ServerVersion
from .version import __version__

GDS = TypeVar("GDS", bound="GraphDataScience")


class UnableToConnectError(Exception):
    pass


class InvalidServerVersionError(Exception):
    pass


class GraphDataScience(DirectEndpoints, UncallableNamespace):
    _AURA_DS_PROTOCOL = "neo4j+s"

    def __init__(self, endpoint: Union[str, QueryRunner], auth: Any = None, aura_ds: bool = False, arrow: bool = True):
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

            neo4j_query_runner = Neo4jQueryRunner(driver, auto_close=True)

            # TODO: Decide what to do if arrow is requested but not present
            try:
                if arrow:
                    arrow_info: Series = neo4j_query_runner.run_query("CALL gds.debug.arrow()").squeeze()
                    if arrow_info["running"]:
                        self._query_runner = ArrowQueryRunner(arrow_info["listenAddress"], neo4j_query_runner, auth)
            finally:
                if not self._query_runner:
                    self._query_runner = neo4j_query_runner

        else:
            self._query_runner = endpoint

        try:
            server_version_string = self._query_runner.run_query("RETURN gds.version()").squeeze()
        except Exception as e:
            raise UnableToConnectError(e)

        server_version_match = re.search(r"^(\d+)\.(\d+)\.(\d+)", server_version_string)
        if not server_version_match:
            raise InvalidServerVersionError(f"{server_version_string} is not a valid GDS library version")
        self._server_version = ServerVersion(*map(int, server_version_match.groups()))

        super().__init__(self._query_runner, "gds", self._server_version)

    def __getattr__(self, attr: str) -> CallBuilder:
        return CallBuilder(self._query_runner, f"gds.{attr}", self._server_version)

    def set_database(self, db: str) -> None:
        self._query_runner.set_database(db)

    def run_cypher(self, query: str, params: Dict[str, Any] = {}) -> DataFrame:
        return self._query_runner.run_query(query, params)

    def driver_config(self) -> Dict[str, Any]:
        return self._config

    @classmethod
    def from_neo4j_driver(cls: Type[GDS], driver: Driver) -> "GraphDataScience":
        query_runner = Neo4jQueryRunner(driver, auto_close=False)
        return cls(query_runner)
