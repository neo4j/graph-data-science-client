from typing import Any, Callable, Dict, Optional

from pandas import DataFrame

from graphdatascience.call_builder import IndirectCallBuilder
from graphdatascience.endpoints import AlphaEndpoints, BetaEndpoints, DirectEndpoints
from graphdatascience.error.uncallable_namespace import UncallableNamespace
from graphdatascience.graph.graph_remote_proc_runner import GraphRemoteProcRunner
from graphdatascience.query_runner.arrow_query_runner import ArrowQueryRunner
from graphdatascience.query_runner.aura_db_arrow_query_runner import (
    AuraDbArrowQueryRunner,
)
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo


class AuraGraphDataScience(DirectEndpoints, UncallableNamespace):
    """
    Primary API class for interacting with Neo4j AuraDB + Graph Data Science.
    Always bind this object to a variable called `gds`.
    """

    def __init__(
        self,
        gds_session_connection_info: DbmsConnectionInfo,
        aura_db_connection_info: DbmsConnectionInfo,
        delete_fn: Callable[[], bool],
        arrow_disable_server_verification: bool = True,
        arrow_tls_root_certs: Optional[bytes] = None,
        bookmarks: Optional[Any] = None,
    ):
        gds_neo4j_query_runner = Neo4jQueryRunner.create(
            gds_session_connection_info.uri, gds_session_connection_info.auth(), aura_ds=True
        )
        gds_query_runner = ArrowQueryRunner.create(
            gds_neo4j_query_runner,
            gds_session_connection_info.auth(),
            gds_neo4j_query_runner.encrypted(),
            arrow_disable_server_verification,
            arrow_tls_root_certs,
        )

        self._server_version = gds_query_runner.server_version()

        if self._server_version < ServerVersion(2, 6, 0):
            raise RuntimeError(
                f"AuraDB connection info was provided but GDS version {self._server_version} \
                    does not support connecting to AuraDB"
            )

        self._db_query_runner = Neo4jQueryRunner.create(
            aura_db_connection_info.uri,
            aura_db_connection_info.auth(),
            aura_ds=True,
            server_version=self._server_version,
        )
        self._db_query_runner.set_bookmarks(bookmarks)

        # we need to explicitly set these as the default value is None
        # which signals the driver to use the default configured database
        # from the dbms.
        gds_query_runner.set_database("neo4j")
        self._db_query_runner.set_database("neo4j")

        self._query_runner = AuraDbArrowQueryRunner(
            gds_query_runner, self._db_query_runner, self._db_query_runner.encrypted(), aura_db_connection_info
        )

        self._delete_fn = delete_fn

        super().__init__(self._query_runner, "gds", self._server_version)

    def run_cypher(
        self, query: str, params: Optional[Dict[str, Any]] = None, database: Optional[str] = None
    ) -> DataFrame:
        """
        Run a Cypher query against the AuraDB instance.

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
        # This will avoid calling valid gds procedures through a raw string
        return self._db_query_runner.run_cypher(query, params, database, False)

    @property
    def graph(self) -> GraphRemoteProcRunner:
        return GraphRemoteProcRunner(self._query_runner, f"{self._namespace}.graph", self._server_version)

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
        self._db_query_runner.set_database(database)

    def set_bookmarks(self, bookmarks: Any) -> None:
        """
        Set Neo4j bookmarks to require a certain state before the next query gets executed

        Parameters
        ----------
        bookmarks: Bookmark(s)
            The Neo4j bookmarks defining the required state
        """
        self._db_query_runner.set_bookmarks(bookmarks)

    def database(self) -> Optional[str]:
        """
        Get the database which queries are run against.

        Returns:
            The name of the database.
        """
        return self._db_query_runner.database()

    def bookmarks(self) -> Optional[Any]:
        """
        Get the Neo4j bookmarks defining the currently required states for queries to execute

        Returns
        -------
        The (possibly None) Neo4j bookmarks defining the currently required state
        """
        return self._db_query_runner.bookmarks()

    def last_bookmarks(self) -> Optional[Any]:
        """
        Get the Neo4j bookmarks defining the state following the most recently called query

        Returns
        -------
        The (possibly None) Neo4j bookmarks defining the state following the most recently called query
        """
        return self._db_query_runner.last_bookmarks()

    def driver_config(self) -> Dict[str, Any]:
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
