import warnings
from typing import Any, Dict, Optional, Tuple, Type, TypeVar, Union

from neo4j import Driver, GraphDatabase
from pandas import DataFrame, Series

from .call_builder import CallBuilder
from .direct_endpoints import DirectEndpoints
from .error.unable_to_connect import UnableToConnectError
from .error.uncallable_namespace import UncallableNamespace
from .query_runner.arrow_query_runner import ArrowQueryRunner
from .query_runner.neo4j_query_runner import Neo4jQueryRunner
from .query_runner.query_runner import QueryRunner
from .server_version.server_version import ServerVersion
from .version import __version__

GDS = TypeVar("GDS", bound="GraphDataScience")


class GraphDataScience(DirectEndpoints, UncallableNamespace):
    _AURA_DS_PROTOCOL = "neo4j+s"

    def __init__(
        self,
        endpoint: Union[str, Driver, QueryRunner],
        auth: Optional[Tuple[str, str]] = None,
        aura_ds: bool = False,
        database: Optional[str] = None,
        arrow: bool = True,
        arrow_disable_server_verification: bool = True,
        arrow_tls_root_certs: Optional[bytes] = None,
    ):
        """
        Parameters
        ----------
        endpoint : Union[str, Driver, QueryRunner]
            The Neo4j endpoint to connect to
        auth : Optional[Tuple[str, str]], default None
            A username, password pair for database authentication.
        aura_ds : bool, default False
            A flag that indicates that that the client is used to connect
            to a Neo4j Aura instance.
        database: Optional[str], default None
            The Neo4j database to query against.
        arrow : bool, default True
            A flag that indicates that the client should use Apache Arrow
            for data streaming if it is available on the server.
        arrow_disable_server_verification : bool, default True
            A flag that indicates that, if the flight client is connecting with
            TLS, that it skips server verification. If this is enabled, all
            other TLS settings are overridden.
        arrow_tls_root_certs : Optional[bytes], default None
            PEM-encoded certificates that are used for the connecting to the
            Arrow Flight server.
        """

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

            self._query_runner = Neo4jQueryRunner(driver, auto_close=True)

        elif isinstance(endpoint, QueryRunner):
            if arrow:
                raise ValueError("Arrow cannot be used if the QueryRunner is provided directly")

            self._query_runner = endpoint

        else:
            driver = endpoint
            self._query_runner = Neo4jQueryRunner(driver, auto_close=False)

        if database:
            self._query_runner.set_database(database)

        try:
            server_version_string = self._query_runner.run_query("RETURN gds.version()").squeeze()
        except Exception as e:
            raise UnableToConnectError(e)
        finally:
            # Some Python versions appear to not call __del__ of self._query_runner when an exception
            # is raised, so we have to close the driver manually.
            if isinstance(endpoint, str):
                driver.close()

        self._server_version = ServerVersion.from_string(server_version_string)
        self._query_runner.set_server_version(self._server_version)

        if arrow and self._server_version >= ServerVersion(2, 1, 0):
            try:
                arrow_info: "Series[Any]" = self._query_runner.run_query("CALL gds.debug.arrow()").squeeze()
                listen_address: str = arrow_info.get(
                    "advertisedListenAddress", arrow_info["listenAddress"]
                )  # type: ignore
                if arrow_info["running"]:
                    self._query_runner = ArrowQueryRunner(
                        listen_address,
                        self._query_runner,
                        self._server_version,
                        auth,
                        driver.encrypted,
                        arrow_disable_server_verification,
                        arrow_tls_root_certs,
                    )
            except Exception as e:
                # AuraDS does not have arrow support at this time, so we should not warn about it.
                # TODO: Remove this check when AuraDS gets arrow support.
                if (
                    "There is no procedure with the name `gds.debug.arrow` "
                    "registered for this database instance." not in str(e)
                ):
                    warnings.warn(f"Could not initialize GDS Flight Server client: {e}")

        super().__init__(self._query_runner, "gds", self._server_version)

    def __getattr__(self, attr: str) -> CallBuilder:
        return CallBuilder(self._query_runner, f"gds.{attr}", self._server_version)

    def set_database(self, database: str) -> None:
        self._query_runner.set_database(database)

    def database(self) -> Optional[str]:
        return self._query_runner.database()

    def run_cypher(
        self, query: str, params: Optional[Dict[str, Any]] = None, database: Optional[str] = None
    ) -> DataFrame:
        qr = self._query_runner

        # The Arrow query runner should not be used to execute arbitary Cypher.
        if isinstance(self._query_runner, ArrowQueryRunner):
            qr = self._query_runner.fallback_query_runner()

        return qr.run_query(query, params, database)

    def driver_config(self) -> Dict[str, Any]:
        return self._config

    @classmethod
    def from_neo4j_driver(
        cls: Type[GDS], driver: Driver, auth: Optional[Tuple[str, str]] = None, arrow: bool = True
    ) -> "GraphDataScience":
        return cls(driver, auth=auth, arrow=arrow)

    def close(self) -> None:
        self._query_runner.close()
