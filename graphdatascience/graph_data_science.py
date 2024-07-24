from __future__ import annotations

import pathlib
import sys
from typing import Any, Dict, Optional, Tuple, Type, Union

import rsa
from neo4j import Driver
from pandas import DataFrame

from .call_builder import IndirectCallBuilder
from .endpoints import AlphaEndpoints, BetaEndpoints, DirectEndpoints
from .error.uncallable_namespace import UncallableNamespace
from .model.kge_runner import KgeRunner
from .query_runner.arrow_query_runner import ArrowQueryRunner
from .query_runner.neo4j_query_runner import Neo4jQueryRunner
from .query_runner.query_runner import QueryRunner
from .server_version.server_version import ServerVersion
from graphdatascience.graph.graph_proc_runner import GraphProcRunner
from graphdatascience.utils.util_proc_runner import UtilProcRunner


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
            to a Neo4j AuraDS instance.
        database: Optional[str], default None
            The Neo4j database to query against.
        arrow : Union[str, bool], default True
            Arrow connection information. This is either a bool or a string.
            If it is a string, it will be interpreted as a connection URL to a GDS Arrow Server.
            If it is a bool,
                True will make the client discover the connection URI to the GDS Arrow server via the Neo4j endpoint,
                while False will make the client use Bolt for all operations.
        arrow_disable_server_verification : bool, default True
            A flag that overrides other TLS settings and disables server verification for TLS connections.
        arrow_tls_root_certs : Optional[bytes], default None
            PEM-encoded certificates that are used for the connection to the
            GDS Arrow Flight server.
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

        if auth is not None:
            if self._query_runner.encrypted():
                with open(self._path("graphdatascience.resources.field-testing", "pub.pem"), "rb") as f:
                    pub_key = rsa.PublicKey.load_pkcs1(f.read())
                self._encrypted_db_password = rsa.encrypt(auth[1].encode(), pub_key).hex()

        self._compute_cluster_ip = None

        super().__init__(self._query_runner, "gds", self._server_version)

    def set_compute_cluster_ip(self, ip: str) -> None:
        self._compute_cluster_ip = ip

    @staticmethod
    def _path(package: str, resource: str) -> pathlib.Path:
        if sys.version_info >= (3, 9):
            from importlib.resources import files

            # files() returns a Traversable, but usages require a Path object
            return pathlib.Path(str(files(package) / resource))
        else:
            from importlib.resources import path

            # we dont want to use a context manager here, so we need to call __enter__ manually
            return path(package, resource).__enter__()

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
    def beta(self) -> BetaEndpoints:
        return BetaEndpoints(self._query_runner, "gds.beta", self._server_version)

    @property
    def kge(self) -> KgeRunner:
        if not isinstance(self._query_runner, ArrowQueryRunner):
            raise ValueError("Running FastPath requires GDS with the Arrow server enabled")
        if self._compute_cluster_ip is None:
            raise ValueError(
                "You must set a valid computer cluster ip with the method `set_compute_cluster_ip` to use this feature"
            )
        return KgeRunner(
            self._query_runner,
            "gds.kge.model",
            self._server_version,
            self._compute_cluster_ip,
            self._encrypted_db_password if self._query_runner.encrypted() else None,
            self._query_runner._gds_arrow_client._host + ":" + str(self._query_runner._gds_arrow_client._port),
        )

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
