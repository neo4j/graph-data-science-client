from typing import Any, Dict, Optional, Tuple

from neo4j import GraphDatabase
from pandas import DataFrame

from .query_runner.neo4j_query_runner import Neo4jQueryRunner
from .server_version.server_version import ServerVersion
from graphdatascience.call_builder import IndirectCallBuilder
from graphdatascience.endpoints import AlphaEndpoints, BetaEndpoints, DirectEndpoints
from graphdatascience.error.uncallable_namespace import UncallableNamespace
from graphdatascience.query_runner.arrow_query_runner import ArrowQueryRunner
from graphdatascience.query_runner.aura_db_arrow_query_runner import (
    AuraDbArrowQueryRunner,
    AuraDbConnectionInfo,
)


class AuraGraphDataScience(DirectEndpoints, UncallableNamespace):
    """
    Primary API class for interacting with Neo4j AuraDB + Graph Data Science.
    Always bind this object to a variable called `gds`.
    """

    def __init__(
        self,
        endpoint: str,
        auth: Tuple[str, str],
        aura_db_connection_info: AuraDbConnectionInfo,
        database: Optional[str] = None,
        arrow_disable_server_verification: bool = True,
        arrow_tls_root_certs: Optional[bytes] = None,
        bookmarks: Optional[Any] = None,
    ):
        gds_query_runner = Neo4jQueryRunner.create(endpoint, auth, aura_ds=True)
        self._driver_config = gds_query_runner.driver_config()
        self._server_version = gds_query_runner.server_version()

        if self._server_version >= ServerVersion(2, 1, 0):
            gds_query_runner = ArrowQueryRunner.create(
                self._server_version,
                gds_query_runner,
                auth,
                gds_query_runner.encrypted(),
                arrow_disable_server_verification,
                arrow_tls_root_certs,
            )

        driver = GraphDatabase.driver(
            aura_db_connection_info.uri, auth=aura_db_connection_info.auth, **self._driver_config
        )
        self._db_query_runner = Neo4jQueryRunner(
            driver, auto_close=True, bookmarks=bookmarks, server_version=self._server_version
        )

        if database:
            self._db_query_runner.set_database(database)

        if self._server_version >= ServerVersion(2, 6, 0):
            self._query_runner = AuraDbArrowQueryRunner(
                gds_query_runner, self._db_query_runner, driver.encrypted, aura_db_connection_info
            )
        else:
            raise RuntimeError(
                f"AuraDB connection info was provided but GDS version {self._server_version} \
                    does not support connecting to AuraDB"
            )

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
        return self._driver_config

    def close(self) -> None:
        """
        Close the GraphDataScience object and release any resources held by it.
        """
        self._query_runner.close()
