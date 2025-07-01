from __future__ import annotations

import os
from typing import Any, Callable, Optional, Union

from pandas import DataFrame

from graphdatascience import QueryRunner, ServerVersion
from graphdatascience.arrow_client.arrow_authentication import ArrowAuthentication
from graphdatascience.arrow_client.arrow_info import ArrowInfo
from graphdatascience.arrow_client.authenticated_arrow_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.write_back_client import WriteBackClient
from graphdatascience.call_builder import IndirectCallBuilder
from graphdatascience.endpoints import (
    AlphaRemoteEndpoints,
    BetaEndpoints,
    DirectEndpoints,
)
from graphdatascience.error.uncallable_namespace import UncallableNamespace
from graphdatascience.graph.graph_remote_proc_runner import GraphRemoteProcRunner
from graphdatascience.procedure_surface.api.wcc_endpoints import WccEndpoints
from graphdatascience.procedure_surface.arrow.wcc_arrow_endpoints import WccArrowEndpoints
from graphdatascience.query_runner.arrow_query_runner import ArrowQueryRunner
from graphdatascience.query_runner.gds_arrow_client import GdsArrowClient
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.query_runner.session_query_runner import SessionQueryRunner
from graphdatascience.query_runner.standalone_session_query_runner import StandaloneSessionQueryRunner
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.utils.util_remote_proc_runner import UtilRemoteProcRunner


class AuraGraphDataScienceFactory:
    """Factory class for creating AuraGraphDataScience instances with all required components."""

    def __init__(
        self,
        session_bolt_connection_info: DbmsConnectionInfo,
        arrow_authentication: Optional[ArrowAuthentication],
        db_endpoint: Optional[Union[Neo4jQueryRunner, DbmsConnectionInfo]],
        delete_fn: Callable[[], bool],
        arrow_disable_server_verification: bool = False,
        arrow_tls_root_certs: Optional[bytes] = None,
        bookmarks: Optional[Any] = None,
        show_progress: bool = True,
    ):
        self.session_bolt_connection_info = session_bolt_connection_info
        self.arrow_authentication = arrow_authentication
        self.db_endpoint = db_endpoint
        self.delete_fn = delete_fn
        self.arrow_disable_server_verification = arrow_disable_server_verification
        self.arrow_tls_root_certs = arrow_tls_root_certs
        self.bookmarks = bookmarks
        self.show_progress = show_progress

    def create(self) -> AuraGraphDataScience:
        """Create and configure an AuraGraphDataScience instance."""
        session_bolt_query_runner = self._create_session_bolt_query_runner()
        arrow_info = ArrowInfo.create(session_bolt_query_runner)
        session_arrow_query_runner = self._create_session_arrow_query_runner(session_bolt_query_runner, arrow_info)
        session_arrow_client = self._create_session_arrow_client(arrow_info, session_bolt_query_runner)
        gds_version = session_bolt_query_runner.server_version()

        session_query_runner: QueryRunner

        if self.db_endpoint is not None:
            db_bolt_query_runner = self._create_db_bolt_query_runner()
            session_query_runner = SessionQueryRunner.create(
                session_arrow_query_runner, db_bolt_query_runner, session_arrow_client, self.show_progress
            )
            wcc_endpoints = self._create_wcc_endpoints(arrow_info, session_bolt_query_runner, db_bolt_query_runner)
        else:
            session_query_runner = StandaloneSessionQueryRunner(session_arrow_query_runner)
            wcc_endpoints = self._create_wcc_endpoints(arrow_info, session_bolt_query_runner, None)

        return AuraGraphDataScience(
            query_runner=session_query_runner,
            wcc_endpoints=wcc_endpoints,
            delete_fn=self.delete_fn,
            gds_version=gds_version,
        )

    def _create_session_bolt_query_runner(self) -> Neo4jQueryRunner:
        return Neo4jQueryRunner.create_for_session(
            endpoint=self.session_bolt_connection_info.uri,
            auth=self.session_bolt_connection_info.get_auth(),
            show_progress=self.show_progress,
        )

    def _create_session_arrow_query_runner(
        self, session_bolt_query_runner: Neo4jQueryRunner, arrow_info: ArrowInfo
    ) -> ArrowQueryRunner:
        return ArrowQueryRunner.create(
            fallback_query_runner=session_bolt_query_runner,
            arrow_info=arrow_info,
            arrow_authentication=self.arrow_authentication,
            encrypted=session_bolt_query_runner.encrypted(),
            disable_server_verification=self.arrow_disable_server_verification,
            tls_root_certs=self.arrow_tls_root_certs,
        )

    def _create_session_arrow_client(
        self, arrow_info: ArrowInfo, session_bolt_query_runner: Neo4jQueryRunner
    ) -> GdsArrowClient:
        return GdsArrowClient.create(
            arrow_info,
            self.arrow_authentication,
            session_bolt_query_runner.encrypted(),
            self.arrow_disable_server_verification,
            self.arrow_tls_root_certs,
        )

    def _create_db_bolt_query_runner(self) -> Neo4jQueryRunner:
        if isinstance(self.db_endpoint, Neo4jQueryRunner):
            db_bolt_query_runner = self.db_endpoint
        elif isinstance(self.db_endpoint, DbmsConnectionInfo):
            db_bolt_query_runner = Neo4jQueryRunner.create_for_db(
                self.db_endpoint.uri,
                self.db_endpoint.get_auth(),
                aura_ds=True,
                show_progress=False,
                database=self.db_endpoint.database,
            )
        else:
            raise ValueError("db_endpoint must be a Neo4jQueryRunner or a DbmsConnectionInfo")

        db_bolt_query_runner.set_bookmarks(self.bookmarks)
        return db_bolt_query_runner

    def _create_wcc_endpoints(
        self, arrow_info: ArrowInfo, session_bolt_query_runner: Neo4jQueryRunner, db_query_runner: Optional[QueryRunner]
    ) -> Optional[WccEndpoints]:
        wcc_endpoints: Optional[WccEndpoints] = None
        if os.environ.get("ENABLE_EXPLICIT_ENDPOINTS") is not None:
            arrow_client = AuthenticatedArrowClient.create(
                arrow_info,
                self.arrow_authentication,
                session_bolt_query_runner.encrypted(),
                self.arrow_disable_server_verification,
                self.arrow_tls_root_certs,
            )

            write_back_client = WriteBackClient(arrow_client, db_query_runner) if db_query_runner is not None else None

            wcc_endpoints = WccArrowEndpoints(arrow_client, write_back_client)
        return wcc_endpoints


class AuraGraphDataScience(DirectEndpoints, UncallableNamespace):
    """
    Primary API class for interacting with Neo4j database + Graph Data Science Session.
    Always bind this object to a variable called `gds`.
    """

    def run_cypher(
        self,
        query: str,
        params: Optional[dict[str, Any]] = None,
        database: Optional[str] = None,
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

        Returns:
            The query result as a DataFrame
        """
        return self._query_runner.run_cypher(query, params, database, False)

    def __init__(
        self,
        query_runner: QueryRunner,
        delete_fn: Callable[[], bool],
        gds_version: ServerVersion,
        wcc_endpoints: Optional[WccEndpoints] = None,
    ):
        self._query_runner = query_runner
        self._delete_fn = delete_fn
        self._server_version = gds_version
        self._wcc_endpoints = wcc_endpoints

        super().__init__(self._query_runner, namespace="gds", server_version=self._server_version)

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
    def wcc(self) -> Union[WccEndpoints, IndirectCallBuilder]:
        if self._wcc_endpoints is None:
            return IndirectCallBuilder(self._query_runner, f"gds.{self._namespace}.wcc", self._server_version)

        return self._wcc_endpoints

    def __getattr__(self, attr: str) -> IndirectCallBuilder:
        return IndirectCallBuilder(self._query_runner, f"gds.{attr}", self._server_version)

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

    def database(self) -> Optional[str]:
        """
        Get the database which cypher queries are run against.

        Returns:
            The name of the database.
        """
        return self._query_runner.database()

    def bookmarks(self) -> Optional[Any]:
        """
        Get the Neo4j bookmarks defining the currently required states for cypher queries to execute

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
