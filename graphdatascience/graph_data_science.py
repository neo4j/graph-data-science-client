from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, Type, Union

from neo4j import Driver
from pandas import DataFrame

from .call_builder import IndirectCallBuilder
from .endpoints import AlphaEndpoints, BetaEndpoints, DirectEndpoints
from .error.uncallable_namespace import UncallableNamespace
from .query_runner.arrow_query_runner import ArrowQueryRunner
from .query_runner.neo4j_query_runner import Neo4jQueryRunner
from .query_runner.query_runner import QueryRunner
from .server_version.server_version import ServerVersion
from graphdatascience.graph.graph_proc_runner import GraphProcRunner


class GraphDataScience(DirectEndpoints, UncallableNamespace):
    """
    Primary API class for the Neo4j Graph Data Science Python Client.
    Always bind this object to a variable called `gds`.
    """

    def __init__(
        self,
        /,
        endpoint: Union[str, Driver, QueryRunner],
        auth: Optional[Tuple[str, str]] = None,
        aura_ds: bool = False,
        database: Optional[str] = None,
        arrow: Union[str, bool] = True,
        arrow_disable_server_verification: bool = True,
        arrow_tls_root_certs: Optional[bytes] = None,
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
        arrow : Union[str, bool], default True
            Arrow connection information. Either a flag that indicates whether the client should use Apache Arrow
            for data streaming if it is available on the server. True means discover the connection URI from the server.
            A connection URI (str) can also be provided.
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
        if aura_ds:
            GraphDataScience._validate_endpoint(endpoint)

        if isinstance(endpoint, QueryRunner):
            self._query_runner = endpoint
        else:
            self._query_runner = Neo4jQueryRunner.create(endpoint, auth, aura_ds, database, bookmarks)

        self._server_version = self._query_runner.server_version()

        if arrow and self._server_version >= ServerVersion(2, 1, 0):
            self._query_runner = ArrowQueryRunner.create(
                self._query_runner,
                auth,
                self._query_runner.encrypted(),
                arrow_disable_server_verification,
                arrow_tls_root_certs,
                None if arrow is True else arrow,
            )

        super().__init__(self._query_runner, "gds", self._server_version)

    @property
    def graph(self) -> GraphProcRunner:
        return GraphProcRunner(self._query_runner, f"{self._namespace}.graph", self._server_version)

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

        return qr.run_cypher(query, params, database, False)

    def driver_config(self) -> Dict[str, Any]:
        """
        Get the configuration used to create the underlying driver used to make queries to Neo4j.

        Returns:
            The configuration as a dictionary.
        """
        return self._query_runner.driver_config()

    @classmethod
    def from_neo4j_driver(
        cls: Type[GraphDataScience],
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

    @staticmethod
    def _validate_endpoint(endpoint: Union[str, Driver, QueryRunner]) -> None:
        if isinstance(endpoint, str):
            protocol = endpoint.split(":")[0]
            if protocol != Neo4jQueryRunner._AURA_DS_PROTOCOL:
                raise ValueError(
                    (
                        f"AuraDS requires using the '{Neo4jQueryRunner._AURA_DS_PROTOCOL}'"
                        f" protocol ('{protocol}' was provided)",
                    )
                )

    def close(self) -> None:
        """
        Close the GraphDataScience object and release any resources held by it.
        """
        self._query_runner.close()
