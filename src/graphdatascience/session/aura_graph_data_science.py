from __future__ import annotations

from typing import Any, Callable

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.gds_arrow_client import GdsArrowClient
from graphdatascience.call_builder import IndirectCallBuilder
from graphdatascience.endpoints import (
    AlphaRemoteEndpoints,
    BetaEndpoints,
    DirectEndpoints,
)
from graphdatascience.error.uncallable_namespace import UncallableNamespace
from graphdatascience.graph.graph_remote_proc_runner import GraphRemoteProcRunner
from graphdatascience.query_runner.arrow_authentication import ArrowAuthentication
from graphdatascience.query_runner.arrow_info import ArrowInfo
from graphdatascience.query_runner.arrow_query_runner import ArrowQueryRunner
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.session_query_runner import SessionQueryRunner
from graphdatascience.query_runner.standalone_session_query_runner import StandaloneSessionQueryRunner
from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.session.session_v2_endpoints import SessionV2Endpoints
from graphdatascience.utils.util_remote_proc_runner import UtilRemoteProcRunner


class AuraGraphDataScience(DirectEndpoints, UncallableNamespace):
    """
    Primary API class for interacting with Neo4j database + Graph Data Science Session.
    Always bind this object to a variable called `gds`.
    """

    @classmethod
    def create(
        cls,
        session_bolt_connection_info: DbmsConnectionInfo,
        arrow_authentication: ArrowAuthentication | None,
        db_endpoint: Neo4jQueryRunner | DbmsConnectionInfo | None,
        delete_fn: Callable[[], bool],
        arrow_client_options: dict[str, Any] | None = None,
        bookmarks: Any | None = None,
        show_progress: bool = True,
    ) -> AuraGraphDataScience:
        session_bolt_query_runner = Neo4jQueryRunner.create_for_session(
            endpoint=session_bolt_connection_info.get_uri(),
            auth=session_bolt_connection_info.get_auth(),
            show_progress=show_progress,
        )

        arrow_info = ArrowInfo.create(session_bolt_query_runner)
        session_arrow_query_runner = ArrowQueryRunner.create(
            fallback_query_runner=session_bolt_query_runner,
            arrow_info=arrow_info,
            arrow_authentication=arrow_authentication,
            encrypted=session_bolt_query_runner.encrypted(),
            arrow_client_options=arrow_client_options,
        )

        session_auth_arrow_client = AuthenticatedArrowClient.create(
            arrow_info=arrow_info,
            auth=arrow_authentication,
            encrypted=session_bolt_query_runner.encrypted(),
            arrow_client_options=arrow_client_options,
        )

        session_arrow_client = GdsArrowClient(flight_client=session_auth_arrow_client)

        gds_version = session_bolt_query_runner.server_version()

        if db_endpoint is not None:
            if isinstance(db_endpoint, Neo4jQueryRunner):
                db_bolt_query_runner = db_endpoint
            else:
                db_bolt_query_runner = Neo4jQueryRunner.create_for_db(
                    db_endpoint.get_uri(),
                    db_endpoint.get_auth(),
                    aura_ds=True,
                    show_progress=False,
                    database=db_endpoint.database,
                )
            db_bolt_query_runner.set_bookmarks(bookmarks)

            session_query_runner = SessionQueryRunner.create(
                session_arrow_query_runner, db_bolt_query_runner, session_arrow_client, show_progress
            )
            return cls(
                query_runner=session_query_runner,
                delete_fn=delete_fn,
                gds_version=gds_version,
                v2_endpoints=SessionV2Endpoints(
                    session_auth_arrow_client, db_bolt_query_runner, show_progress=show_progress
                ),
                authenticated_arrow_client=session_auth_arrow_client,
            )
        else:
            standalone_query_runner = StandaloneSessionQueryRunner(session_arrow_query_runner)
            return cls(
                query_runner=standalone_query_runner,
                delete_fn=delete_fn,
                gds_version=gds_version,
                v2_endpoints=SessionV2Endpoints(session_auth_arrow_client, None, show_progress=show_progress),
                authenticated_arrow_client=session_auth_arrow_client,
            )

    def __init__(
        self,
        query_runner: QueryRunner,
        delete_fn: Callable[[], bool],
        gds_version: ServerVersion,
        v2_endpoints: SessionV2Endpoints,
        authenticated_arrow_client: AuthenticatedArrowClient,
    ):
        self._query_runner = query_runner
        self._delete_fn = delete_fn
        self._server_version = gds_version
        self._v2_endpoints = v2_endpoints
        self._authenticated_arrow_client = authenticated_arrow_client

        super().__init__(self._query_runner, namespace="gds", server_version=self._server_version)

    def run_cypher(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        database: str | None = None,
        retryable: bool = False,
        mode: QueryMode = QueryMode.WRITE,
    ) -> DataFrame:
        """
        Run a Cypher query against the Neo4j database.

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
        if retryable:
            return self._query_runner.run_retryable_cypher(query, params, database, custom_error=False, mode=mode)
        else:
            return self._query_runner.run_cypher(query, params, database, custom_error=False, mode=mode)

    @property
    def graph(self) -> GraphRemoteProcRunner:
        return GraphRemoteProcRunner(self._query_runner, f"{self._namespace}.graph", self._server_version)

    @property
    def util(self) -> UtilRemoteProcRunner:
        return UtilRemoteProcRunner(self._query_runner, f"{self._namespace}.util", self._server_version)

    @property
    def alpha(self) -> AlphaRemoteEndpoints:
        return AlphaRemoteEndpoints(self._query_runner, "gds.alpha", self._server_version)

    @property
    def beta(self) -> BetaEndpoints:
        return BetaEndpoints(self._query_runner, "gds.beta", self._server_version)

    @property
    def v2(self) -> SessionV2Endpoints:
        """
        Return preview v2 endpoints. These endpoints may change without warning.
        These endpoints are a preview of the API for the next major version of this library.
        """
        return self._v2_endpoints

    def __getattr__(self, attr: str) -> IndirectCallBuilder:
        return IndirectCallBuilder(self._query_runner, f"gds.{attr}", self._server_version)

    def arrow_client(self) -> GdsArrowClient:
        """
        Returns a GdsArrowClient that is authenticated to communicate with the Aura Graph Analytics Session.
        This client can be used to get direct access to the specific session's Arrow Flight server.

        Returns:
            A GdsArrowClient
        -------

        """
        return GdsArrowClient(self._authenticated_arrow_client)

    def set_database(self, database: str) -> None:
        """
        Set the database which cypher queries are run against.

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
        self._v2_endpoints.set_show_progress(show_progress)

    def database(self) -> str | None:
        """
        Get the database which cypher queries are run against.

        Returns:
            The name of the database.
        """
        return self._query_runner.database()

    def bookmarks(self) -> Any | None:
        """
        Get the Neo4j bookmarks defining the currently required states for cypher queries to execute

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

    def driver_config(self) -> dict[str, Any]:
        """
        Get the configuration used to create the underlying driver used to make queries to Neo4j.

        Returns:
            The configuration as a dictionary.
        """
        return self._query_runner.driver_config()

    def delete(self) -> bool:
        """
        Delete a GDS session.
        """
        self.close()
        return self._delete_fn()

    def close(self) -> None:
        """
        Close the GraphDataScience object and release any resources held by it.
        """
        self._query_runner.close()
