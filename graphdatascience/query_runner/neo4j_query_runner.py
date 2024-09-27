from __future__ import annotations

import logging
import re
import time
import warnings
from typing import Any, Dict, List, Optional, Tuple, Union

import neo4j
from pandas import DataFrame

from ..call_parameters import CallParameters
from ..error.endpoint_suggester import generate_suggestive_error_message
from ..error.gds_not_installed import GdsNotFound
from ..error.unable_to_connect import UnableToConnectError
from ..server_version.server_version import ServerVersion
from ..version import __version__
from .cypher_graph_constructor import CypherGraphConstructor
from .graph_constructor import GraphConstructor
from .progress.query_progress_logger import QueryProgressLogger
from .query_runner import QueryRunner


class Neo4jQueryRunner(QueryRunner):
    _AURA_DS_PROTOCOL = "neo4j+s"
    _LOG_POLLING_INTERVAL = 0.5
    _NEO4J_DRIVER_VERSION = ServerVersion.from_string(neo4j.__version__)

    @staticmethod
    def create(
        endpoint: Union[str, neo4j.Driver],
        auth: Optional[Tuple[str, str]] = None,
        aura_ds: bool = False,
        database: Optional[str] = None,
        bookmarks: Optional[Any] = None,
        show_progress: bool = True,
    ) -> Neo4jQueryRunner:
        if isinstance(endpoint, str):
            config: Dict[str, Any] = {"user_agent": f"neo4j-graphdatascience-v{__version__}"}

            if aura_ds:
                Neo4jQueryRunner._configure_aura(config)

            driver = neo4j.GraphDatabase.driver(endpoint, auth=auth, **config)

            query_runner = Neo4jQueryRunner(
                driver,
                auto_close=True,
                bookmarks=bookmarks,
                config=config,
                database=database,
            )

        elif isinstance(endpoint, neo4j.Driver):
            query_runner = Neo4jQueryRunner(
                endpoint, auto_close=False, bookmarks=bookmarks, database=database, show_progress=show_progress
            )

        else:
            raise ValueError(f"Invalid endpoint type: {type(endpoint)}")

        if Neo4jQueryRunner._NEO4J_DRIVER_VERSION >= ServerVersion(5, 21, 0):
            notifications_logger = logging.getLogger("neo4j.notifications")
            # the client does not expose YIELD fields so we just skip these warnings for now
            notifications_logger.addFilter(
                lambda record: (
                    "The query used a deprecated field from a procedure" in record.msg and "by 'gds." in record.msg
                )
            )

        return query_runner

    @staticmethod
    def _configure_aura(config: Dict[str, Any]) -> None:
        config["max_connection_lifetime"] = 60 * 8  # 8 minutes
        config["keep_alive"] = True
        config["max_connection_pool_size"] = 50

    def __init__(
        self,
        driver: neo4j.Driver,
        config: Dict[str, Any] = {},
        database: Optional[str] = neo4j.DEFAULT_DATABASE,
        auto_close: bool = False,
        bookmarks: Optional[Any] = None,
        show_progress: bool = True,
    ):
        self._driver = driver
        self._config = config
        self._auto_close = auto_close
        self._database = database
        self._logger = logging.getLogger()
        self._bookmarks = bookmarks
        self._last_bookmarks: Optional[Any] = None
        self._server_version = None
        self._show_progress = show_progress
        self._progress_logger = QueryProgressLogger(
            self.__run_cypher_simplified_for_query_progress_logger, self.server_version
        )

    def __run_cypher_simplified_for_query_progress_logger(self, query: str, database: Optional[str]) -> DataFrame:
        return self.run_cypher(query=query, database=database)

    def run_cypher(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
        custom_error: bool = True,
    ) -> DataFrame:
        if params is None:
            params = {}

        if database is None:
            database = self._database

        self._verify_connectivity(database=database)

        with self._driver.session(database=database, bookmarks=self.bookmarks()) as session:
            try:
                result = session.run(query, params)
            except Exception as e:
                if custom_error:
                    self.handle_driver_exception(session, e)
                else:
                    raise e

            # Though pandas support may be experimental in the `neo4j` package, it should always
            # be supported in the `graphdatascience` package.
            warnings.filterwarnings(
                "ignore",
                message=r"^pandas support is experimental and might be changed or removed in future versions$",
            )

            df = result.to_df()

            if self._NEO4J_DRIVER_VERSION < ServerVersion(5, 0, 0):
                self._last_bookmarks = [session.last_bookmark()]
            else:
                self._last_bookmarks = session.last_bookmarks()

            if (
                Neo4jQueryRunner._NEO4J_DRIVER_VERSION >= ServerVersion(5, 21, 0)
                and result._warn_notification_severity == "WARNING"
            ):
                # the client does not expose YIELD fields so we just skip these warnings for now
                warnings.filterwarnings(
                    "ignore", message=r".*The query used a deprecated field from a procedure\. .* by 'gds.* "
                )
            else:
                notifications = result.consume().notifications
                if notifications:
                    for notification in notifications:
                        self._forward_cypher_warnings(notification)

            return df

    def call_function(self, endpoint: str, params: Optional[CallParameters] = None) -> Any:
        if params is None:
            params = CallParameters()
        query = f"RETURN {endpoint}({params.placeholder_str()})"

        return self.run_cypher(query, params).squeeze()

    def call_procedure(
        self,
        endpoint: str,
        params: Optional[CallParameters] = None,
        yields: Optional[List[str]] = None,
        database: Optional[str] = None,
        logging: bool = False,
        custom_error: bool = True,
    ) -> DataFrame:
        if params is None:
            params = CallParameters()

        yields_clause = "" if yields is None else " YIELD " + ", ".join(yields)
        query = f"CALL {endpoint}({params.placeholder_str()}){yields_clause}"

        def run_cypher_query() -> DataFrame:
            return self.run_cypher(query, params, database, custom_error)

        if self._resolve_show_progress(logging):
            job_id = self._progress_logger.extract_or_create_job_id(params)
            return self._progress_logger.run_with_progress_logging(run_cypher_query, job_id, database)
        else:
            return run_cypher_query()

    def _resolve_show_progress(self, show_progress: bool) -> bool:
        if self._show_progress is False:
            return False

        return show_progress

    def server_version(self) -> ServerVersion:
        if self._server_version:
            return self._server_version

        try:
            server_version_string = self.run_cypher("RETURN gds.version()", custom_error=False).squeeze()
            self._server_version = ServerVersion.from_string(server_version_string)
            return self._server_version
        except Exception as e:
            if "Unknown function 'gds.version'" in str(e):
                # Some Python versions appear to not call __del__ of self._query_runner when an exception
                # is raised, so we have to close the driver manually.
                self._driver.close()
                # if isinstance(endpoint, str):
                #    driver.close()

                raise GdsNotFound(
                    """The Graph Data Science library is not correctly installed on the Neo4j server.
                    Please refer to https://neo4j.com/docs/graph-data-science/current/installation/.
                    """
                )

            raise UnableToConnectError(e)

    def encrypted(self) -> bool:
        return self._driver.encrypted

    def driver_config(self) -> Dict[str, Any]:
        return self._config

    def _forward_cypher_warnings(self, notification: Dict[str, Any]) -> None:
        # (see https://neo4j.com/docs/status-codes/current/notifications/ for more details)
        severity = notification["severity"]
        if severity == "WARNING":
            if "query used a deprecated field from a procedure" in notification["description"]:
                # the client does not expose YIELD fields so we just skip these warnings for now
                return

            if "deprecated" in notification["description"]:
                warning: Warning = DeprecationWarning(notification["description"])
            else:
                warning = RuntimeWarning(notification["description"])
            warnings.warn(warning)
        elif severity == "INFORMATION":
            self._logger.info(notification)

    def set_database(self, database: str) -> None:
        self._database = database

    def set_bookmarks(self, bookmarks: Optional[Any]) -> None:
        self._bookmarks = bookmarks

    def close(self) -> None:
        self._driver.close()

    def database(self) -> Optional[str]:
        return self._database

    def bookmarks(self) -> Optional[Any]:
        return self._bookmarks

    def last_bookmarks(self) -> Optional[Any]:
        return self._last_bookmarks

    def __del__(self) -> None:
        if self._auto_close:
            self._driver.close()

    def create_graph_constructor(
        self, graph_name: str, concurrency: int, undirected_relationship_types: Optional[List[str]]
    ) -> GraphConstructor:
        return CypherGraphConstructor(
            self, graph_name, concurrency, undirected_relationship_types, self.server_version()
        )

    def set_show_progress(self, show_progress: bool) -> None:
        self._show_progress = show_progress

    @staticmethod
    def handle_driver_exception(session: neo4j.Session, e: Exception) -> None:
        reg_gds_hit = re.search(
            r"There is no procedure with the name `(gds(?:\.\w+)+)` registered for this database instance",
            str(e),
        )
        if not reg_gds_hit:
            raise e

        requested_endpoint = reg_gds_hit.group(1)

        list_result = session.run("CALL gds.list() YIELD name")
        all_endpoints = list_result.to_df()["name"].tolist()

        raise SyntaxError(generate_suggestive_error_message(requested_endpoint, all_endpoints)) from e

    def _verify_connectivity(self, database: Optional[str] = None) -> None:
        WAIT_TIME = 1
        MAX_RETRYS = 10 * 60
        WARN_INTERVAL = 10

        if database is None:
            database = self._database

        exception = None
        retrys = 0
        while retrys < MAX_RETRYS:
            try:
                if self._NEO4J_DRIVER_VERSION < ServerVersion(5, 0, 0):
                    warnings.filterwarnings(
                        "ignore",
                        category=neo4j.ExperimentalWarning,
                        message=r"^The configuration may change in the future.$",
                    )
                else:
                    warnings.filterwarnings(
                        "ignore",
                        category=neo4j.ExperimentalWarning,
                        message=(
                            r"^All configuration key-word arguments to verify_connectivity\(\) are experimental. "
                            "They might be changed or removed in any future version without prior notice.$"
                        ),
                    )
                self._driver.verify_connectivity(database=database)
                break
            except neo4j.exceptions.DriverError as e:
                exception = e
                if retrys % WARN_INTERVAL == 0:
                    self._logger.warning("Unable to connect to the Neo4j DBMS. Trying again...")

                time.sleep(WAIT_TIME)
                retrys += 1

                continue

        if retrys == MAX_RETRYS:
            raise UnableToConnectError("Unable to connect to the Neo4j DBMS") from exception
