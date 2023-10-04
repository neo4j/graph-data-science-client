import warnings
from typing import Any, Dict, Optional, Tuple, Type, TypeVar, Union

from neo4j import Driver, GraphDatabase
from pandas import DataFrame, Series

from .call_builder import IndirectCallBuilder
from .endpoints import AlphaEndpoints, BetaEndpoints, DirectEndpoints
from .error.gds_not_installed import GdsNotFound
from .error.unable_to_connect import UnableToConnectError
from .error.uncallable_namespace import UncallableNamespace
from .query_runner.arrow_query_runner import ArrowQueryRunner
from .query_runner.neo4j_query_runner import Neo4jQueryRunner
from .query_runner.query_runner import QueryRunner
from .server_version.server_version import ServerVersion
from .version import __version__
from graphdatascience.query_runner.aura_db_arrow_query_runner import (
    AuraDbArrowQueryRunner,
    AuraDbConnectionInfo,
)

GDS = TypeVar("GDS", bound="GraphDataScience")


class GraphDataScience(DirectEndpoints, UncallableNamespace):
    """
    Primary API class for the Neo4j Graph Data Science Python Client.
    Always bind this object to a variable called `gds`.
    """

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
        aura_db_connection_info: Optional[AuraDbConnectionInfo] = None,
        bookmarks: Optional[Any] = None,
    ):
        """
        Construct a new GraphDataScience object.

        Parameters
        ----------
        endpoint : Union[str, Driver, QueryRunner]
            The Neo4j endpoint to connect to. Most commonly, this is a Bolt connection URI.
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
        bookmarks : Optional[Any], default None
            The Neo4j bookmarks to require a certain state before the next query gets executed.
        """

        if isinstance(endpoint, str):
            self._config: Dict[str, Any] = {"user_agent": f"neo4j-graphdatascience-v{__version__}"}

            if aura_ds:
                self._configure_aura(endpoint, self._config)

            driver = GraphDatabase.driver(endpoint, auth=auth, **self._config)

            self._query_runner = Neo4jQueryRunner(driver, auto_close=True, bookmarks=bookmarks)

        elif isinstance(endpoint, QueryRunner):
            if arrow:
                raise ValueError("Arrow cannot be used if the QueryRunner is provided directly")

            self._query_runner = endpoint

        else:
            driver = endpoint
            self._query_runner = Neo4jQueryRunner(driver, auto_close=False, bookmarks=bookmarks)

        if database:
            self._query_runner.set_database(database)

        try:
            server_version_string = self._query_runner.run_query("RETURN gds.version()", custom_error=False).squeeze()
        except Exception as e:
            if "Unknown function 'gds.version'" in str(e):
                # Some Python versions appear to not call __del__ of self._query_runner when an exception
                # is raised, so we have to close the driver manually.
                if isinstance(endpoint, str):
                    driver.close()

                raise GdsNotFound(
                    """The Graph Data Science library is not correctly installed on the Neo4j server.
                    Please refer to https://neo4j.com/docs/graph-data-science/current/installation/.
                    """
                )

            raise UnableToConnectError(e)

        self._server_version = ServerVersion.from_string(server_version_string)
        self._query_runner.set_server_version(self._server_version)

        if arrow and self._server_version >= ServerVersion(2, 1, 0):
            yield_fields = (
                "running, listenAddress"
                if self._server_version >= ServerVersion(2, 2, 1)
                else "running, advertisedListenAddress"
            )
            arrow_info: "Series[Any]" = self._query_runner.run_query(
                f"CALL gds.debug.arrow() YIELD {yield_fields}", custom_error=False
            ).squeeze()
            listen_address: str = arrow_info.get("advertisedListenAddress", arrow_info["listenAddress"])  # type: ignore
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
        if aura_db_connection_info:
            if self._server_version >= ServerVersion(2, 4, 0):
                self._query_runner = AuraDbArrowQueryRunner(self._query_runner, aura_db_connection_info)
            else:
                warnings.warn(
                    f"AuraDB connection info was provided but GDS version {self._server_version} \
                        does not support connecting to AuraDB"
                )

        super().__init__(self._query_runner, "gds", self._server_version)

    @property
    def alpha(self) -> AlphaEndpoints:
        return AlphaEndpoints(self._query_runner, "gds.alpha", self._server_version)

    @property
    def beta(self) -> BetaEndpoints:
        return BetaEndpoints(self._query_runner, "gds.beta", self._server_version)

    def __getattr__(self, attr: str) -> IndirectCallBuilder:
        return IndirectCallBuilder(self._query_runner, f"gds.{attr}", self._server_version)

    def set_database(self, database: str) -> None:
        """
        Set the database which queries are run against.

        Parameters
        -------
        database: str
            The name of the database to run queries against.
        """
        self._query_runner.set_database(database)

    def set_bookmarks(self, bookmarks: Any) -> None:
        """
        Set Neo4j bookmarks to require a certain state before the next query gets executed

        Parameters
        ----------
        bookmarks: Bookmark(s)
            The Neo4j bookmarks defining the required state
        """
        self._query_runner.set_bookmarks(bookmarks)

    def database(self) -> Optional[str]:
        """
        Get the database which queries are run against.

        Returns:
            The name of the database.
        """
        return self._query_runner.database()

    def bookmarks(self) -> Optional[Any]:
        """
        Get the Neo4j bookmarks defining the currently required states for queries to execute

        Returns
        -------
        The (possibly None) Neo4j bookmarks defining the currently required state
        """
        return self._query_runner.bookmarks()

    def last_bookmarks(self) -> Optional[Any]:
        """
        Get the Neo4j bookmarks defining the state following the most recently called query

        Returns
        -------
        The (possibly None) Neo4j bookmarks defining the state following the most recently called query
        """
        return self._query_runner.last_bookmarks()

    def run_cypher(
        self, query: str, params: Optional[Dict[str, Any]] = None, database: Optional[str] = None
    ) -> DataFrame:
        """
        Run a Cypher query

        Parameters
        ----------
        query: str
            the Cypher query
        params: Dict[str, Any]
            parameters to the query
        database: str
            the database on which to run the query

        Returns:
            The query result as a DataFrame
        """
        qr = self._query_runner

        # The Arrow query runner should not be used to execute arbitrary Cypher
        if isinstance(self._query_runner, ArrowQueryRunner):
            qr = self._query_runner.fallback_query_runner()

        return qr.run_query(query, params, database, False)

    def driver_config(self) -> Dict[str, Any]:
        """
        Get the configuration used to create the underlying driver used to make queries to Neo4j.

        Returns:
            The configuration as a dictionary.
        """
        return self._config

    @classmethod
    def from_neo4j_driver(
        cls: Type[GDS],
        driver: Driver,
        auth: Optional[Tuple[str, str]] = None,
        database: Optional[str] = None,
        arrow: bool = True,
        arrow_disable_server_verification: bool = True,
        arrow_tls_root_certs: Optional[bytes] = None,
        bookmarks: Optional[Any] = None,
    ) -> "GraphDataScience":
        return cls(
            driver,
            auth=auth,
            database=database,
            arrow=arrow,
            arrow_disable_server_verification=arrow_disable_server_verification,
            arrow_tls_root_certs=arrow_tls_root_certs,
            bookmarks=bookmarks,
        )

    def close(self) -> None:
        """
        Close the GraphDataScience object and release any resources held by it.
        """
        self._query_runner.close()

    @classmethod
    def _configure_aura(cls, uri: str, config: Dict[str, Any]) -> None:
        protocol = uri.split(":")[0]
        if not protocol == cls._AURA_DS_PROTOCOL:
            raise ValueError(
                f"AuraDS requires using the '{cls._AURA_DS_PROTOCOL}' protocol ('{protocol}' was provided)"
            )

        config["max_connection_lifetime"] = 60 * 8  # 8 minutes
        config["keep_alive"] = True
        config["max_connection_pool_size"] = 50
