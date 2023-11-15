from typing import Any, Dict, Optional, Tuple

from neo4j import GraphDatabase
from pandas import DataFrame

from .query_runner.neo4j_query_runner import Neo4jQueryRunner
from .server_version.server_version import ServerVersion
from graphdatascience import GraphDataScience
from graphdatascience.query_runner.aura_db_arrow_query_runner import (
    AuraDbArrowQueryRunner,
    AuraDbConnectionInfo,
)


class AuraGraphDataScience(GraphDataScience):
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
        super().__init__(
            endpoint=endpoint,
            auth=auth,
            aura_ds=True,
            database=database,
            arrow=True,
            arrow_disable_server_verification=arrow_disable_server_verification,
            arrow_tls_root_certs=arrow_tls_root_certs,
            bookmarks=bookmarks,
        )

        gds_query_runner = self._query_runner

        driver = GraphDatabase.driver(aura_db_connection_info.uri, auth=aura_db_connection_info.auth, **self._config)
        self._db_query_runner = Neo4jQueryRunner(driver, auto_close=True, bookmarks=bookmarks)
        self._db_query_runner.set_server_version(self._server_version)

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
        return self._db_query_runner.run_query(query, params, database, False)
