from __future__ import annotations

import warnings
from types import TracebackType
from typing import Any, Type

import neo4j
from neo4j import Driver
from pandas import DataFrame

from graphdatascience.plugin_v2_endpoints import PluginV2Endpoints
from graphdatascience.query_runner.arrow_authentication import UsernamePasswordAuthentication
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.topological_lp.topological_lp_runner import TopologicalLPRunner

from .call_builder import IndirectCallBuilder
from .endpoints import AlphaEndpoints, BetaEndpoints, DirectEndpoints
from .error.uncallable_namespace import UncallableNamespace
from .graph.graph_proc_runner import GraphProcRunner
from .query_runner.arrow_info import ArrowInfo
from .query_runner.arrow_query_runner import ArrowQueryRunner
from .query_runner.neo4j_query_runner import Neo4jQueryRunner
from .query_runner.query_runner import QueryRunner
from .server_version.server_version import ServerVersion
from .utils.util_proc_runner import UtilProcRunner
from .version import __min_server_version__


class GraphDataScience(DirectEndpoints, UncallableNamespace):
    """
    Primary API class for the Neo4j Graph Data Science Python Client.
    Always bind this object to a variable called `gds`.
    """

    def __init__(
        self,
        /,
        endpoint: str | Driver | QueryRunner,
        auth: tuple[str, str] | None = None,
        aura_ds: bool = False,
        database: str | None = None,
        arrow: str | bool = True,
        arrow_disable_server_verification: bool = True,
        arrow_tls_root_certs: bytes | None = None,
        bookmarks: Any | None = None,
        show_progress: bool = True,
        arrow_client_options: dict[str, Any] | None = None,
    ):
        """
        Construct a new GraphDataScience object.

        Parameters
        ----------
        endpoint : str | Driver | QueryRunner
            The Neo4j endpoint to connect to. Most commonly, this is a Bolt connection URI.
        auth : tuple[str, str] | None, default None
            A username, password pair for database authentication.
        aura_ds : bool, default False
            A flag that indicates that that the client is used to connect
            to a Neo4j AuraDS instance.
        database: str | None, default None
            The Neo4j database to query against.
        arrow : str | bool, default True
            Arrow connection information. This is either a string or a bool.

            - If it is a string, it will be interpreted as a connection URL to a GDS Arrow Server.
            - If it is a bool:
                - True will make the client discover the connection URI to the GDS Arrow server via the Neo4j endpoint.
                - False will make the client use Bolt for all operations.
        arrow_disable_server_verification : bool, default True
            .. deprecated:: 1.16
                Use arrow_client_options instead

            A flag that overrides other TLS settings and disables server verification for TLS connections.
        arrow_tls_root_certs : bytes | None, default None
            .. deprecated:: 1.16
                Use arrow_client_options instead

            PEM-encoded certificates that are used for the connection to the
            GDS Arrow Flight server.
        bookmarks : Any | None, default None
            The Neo4j bookmarks to require a certain state before the next query gets executed.
        show_progress : bool, default True
            A flag to indicate whether to show progress bars for running procedures.
        arrow_client_options : dict[str, Any] | None, default None
            Additional options to be passed to the Arrow Flight client.
        """
        if aura_ds:
            GraphDataScience._validate_endpoint(endpoint)

        neo4j_query_runner: Neo4jQueryRunner | None = None
        if isinstance(endpoint, QueryRunner):
            self._query_runner = endpoint
        else:
            db_auth = None
            if auth:
                db_auth = neo4j.basic_auth(*auth)
            neo4j_query_runner = Neo4jQueryRunner.create_for_db(
                endpoint, db_auth, aura_ds, database, bookmarks, show_progress
            )
            self._query_runner = neo4j_query_runner

        self._server_version = self._query_runner.server_version()

        if self._server_version < ServerVersion.from_string(__min_server_version__):
            warnings.warn(
                DeprecationWarning(
                    f"Client does not support the given server version `{self._server_version}`."
                    + " We recommend to either update the GDS server version or use a compatible version of the `graphdatascience` package."
                    + " Please refer to the compatibility matrix at https://neo4j.com/docs/graph-data-science-client/current/installation/#python-client-system-requirements."
                )
            )

        arrow_info = ArrowInfo.create(self._query_runner)
        if arrow and arrow_info.enabled and self._server_version >= ServerVersion(2, 1, 0):
            arrow_auth = None
            if auth is not None:
                username, password = auth
                arrow_auth = UsernamePasswordAuthentication(username, password)

            if arrow_client_options is None:
                arrow_client_options = {}
            if arrow_disable_server_verification:
                arrow_client_options["disable_server_verification"] = True
            if arrow_tls_root_certs is not None:
                arrow_client_options["tls_root_certs"] = arrow_tls_root_certs
            self._query_runner = ArrowQueryRunner.create(
                self._query_runner,
                arrow_info=arrow_info,
                arrow_authentication=arrow_auth,
                encrypted=self._query_runner.encrypted(),
                arrow_client_options=arrow_client_options,
                connection_string_override=None if arrow is True else arrow,
            )

        arrow_client = (
            None if not isinstance(self._query_runner, ArrowQueryRunner) else self._query_runner._gds_arrow_client
        )
        if neo4j_query_runner:
            self._v2_endpoints: PluginV2Endpoints | None = PluginV2Endpoints(neo4j_query_runner, arrow_client)
        else:
            self._v2_endpoints = None

        self._query_runner.set_show_progress(show_progress)
        super().__init__(self._query_runner, namespace="gds", server_version=self._server_version)

    @property
    def graph(self) -> GraphProcRunner:
        return GraphProcRunner(self._query_runner, f"{self._namespace}.graph", self._server_version)

    @property
    def util(self) -> UtilProcRunner:
        return UtilProcRunner(self._query_runner, f"{self._namespace}.util", self._server_version)

    @property
    def alpha(self) -> AlphaEndpoints:
        return AlphaEndpoints(self._query_runner, "gds.alpha", self._server_version)

    @property
    def linkprediction(self) -> TopologicalLPRunner:
        return TopologicalLPRunner(self._query_runner, f"{self._namespace}.linkprediction", self._server_version)

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

    def set_show_progress(self, show_progress: bool) -> None:
        """
        Set whether to show progress for running procedures.

        Parameters
        ----------
        show_progress: bool
            Whether to show progress for procedures.
        """
        self._query_runner.set_show_progress(show_progress)

    def database(self) -> str | None:
        """
        Get the database which queries are run against.

        Returns:
            The name of the database.
        """
        return self._query_runner.database()

    def bookmarks(self) -> Any | None:
        """
        Get the Neo4j bookmarks defining the currently required states for queries to execute

        Returns
        -------
        The (possibly None) Neo4j bookmarks defining the currently required state
        """
        return self._query_runner.bookmarks()

    def last_bookmarks(self) -> Any | None:
        """
        Get the Neo4j bookmarks defining the state following the most recently called query

        Returns
        -------
        The (possibly None) Neo4j bookmarks defining the state following the most recently called query
        """
        return self._query_runner.last_bookmarks()

    def run_cypher(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        database: str | None = None,
        retryable: bool = False,
        mode: QueryMode = QueryMode.WRITE,
    ) -> DataFrame:
        """
        Run a Cypher query

        Parameters
        ----------
        query: str
            the Cypher query
        params: dict[str, Any]
            parameters to the query
        database: str
            the database on which to run the query
        retryable: bool
            whether the query can be automatically retried. Make sure the query is idempotent if set to True.
        mode: QueryMode
            the query mode to use (READ or WRITE). Set based on the operation performed in the query.

        Returns:
            The query result as a DataFrame
        """
        qr = self._query_runner

        # The Arrow query runner should not be used to execute arbitrary Cypher
        if isinstance(self._query_runner, ArrowQueryRunner):
            qr = self._query_runner.fallback_query_runner()

        if retryable:
            return qr.run_retryable_cypher(query, params, database, custom_error=False, mode=mode)
        else:
            return qr.run_cypher(query, params, database, custom_error=False, mode=mode)

    def driver_config(self) -> dict[str, Any]:
        """
        Get the configuration used to create the underlying driver used to make queries to Neo4j.

        Returns:
            The configuration as a dictionary.
        """
        return self._query_runner.driver_config()

    @property
    def v2(self) -> PluginV2Endpoints:
        """
        Return preview v2 endpoints. These endpoints may change without warning.
        These endpoints are a preview of the API for the next major version of this library.
        """
        if not self._v2_endpoints:
            raise RuntimeError("v2 endpoints are not available.")

        return self._v2_endpoints

    @classmethod
    def from_neo4j_driver(
        cls: Type[GraphDataScience],
        driver: Driver,
        auth: tuple[str, str] | None = None,
        database: str | None = None,
        arrow: str | bool = True,
        arrow_disable_server_verification: bool = True,
        arrow_tls_root_certs: bytes | None = None,
        bookmarks: Any | None = None,
        arrow_client_options: dict[str, Any] | None = None,
    ) -> GraphDataScience:
        """
        Construct a new GraphDataScience object from an existing Neo4j Driver.
        This method is useful when you already have a Neo4j Driver instance and want to use it with the GDS client.

        Parameters
        ----------
        driver: Driver
            The Neo4j Driver instance to use.
        auth : tuple[str, str] | None, default None
            A username, password pair for authentication.
        database: str | None, default None
            The Neo4j database to query against.
        arrow : str | bool, default True
            Arrow connection information. This is either a string or a bool.

            - If it is a string, it will be interpreted as a connection URL to a GDS Arrow Server.
            - If it is a bool:
                - True will make the client discover the connection URI to the GDS Arrow server via the Neo4j endpoint.
                - False will make the client use Bolt for all operations.
        arrow_disable_server_verification : bool, default True
            .. deprecated:: 1.16
                Use arrow_client_options instead

            A flag that overrides other TLS settings and disables server verification for TLS connections.
        arrow_tls_root_certs : bytes | None, default None
            .. deprecated:: 1.16
                Use arrow_client_options instead

            PEM-encoded certificates that are used for the connection to the
            GDS Arrow Flight server.
        bookmarks : Any | None, default None
            The Neo4j bookmarks to require a certain state before the next query gets executed.
        show_progress : bool, default True
            A flag to indicate whether to show progress bars for running procedures.
        arrow_client_options : dict[str, Any] | None, default None
            Additional options to be passed to the Arrow Flight client.
        Returns:
            A new GraphDataScience object. configured with the provided Neo4j Driver.
        """
        return cls(
            driver,
            auth=auth,
            database=database,
            arrow=arrow,
            arrow_disable_server_verification=arrow_disable_server_verification,
            arrow_tls_root_certs=arrow_tls_root_certs,
            bookmarks=bookmarks,
            arrow_client_options=arrow_client_options,
        )

    @staticmethod
    def _validate_endpoint(endpoint: str | Driver | QueryRunner) -> None:
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

        If the GraphDataScience object was instantiated with a Neo4j Driver, the driver will not be closed as we cannot assume sole ownership of it.
        """
        self._query_runner.close()

    def __enter__(self) -> GraphDataScience:
        return self

    def __exit__(
        self,
        exception_type: Type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
