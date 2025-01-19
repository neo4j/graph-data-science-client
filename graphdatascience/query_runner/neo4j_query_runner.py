from __future__ import annotations

import logging
import re
import time
import warnings
from typing import Any, NamedTuple, Optional, Union

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
    def create_for_db(
        endpoint: Union[str, neo4j.Driver],
        auth: Optional[tuple[str, str]] = None,
        aura_ds: bool = False,
        database: Optional[str] = None,
        bookmarks: Optional[Any] = None,
        show_progress: bool = True,
    ) -> Neo4jQueryRunner:
        if isinstance(endpoint, str):
            config: dict[str, Any] = {"user_agent": f"neo4j-graphdatascience-v{__version__}"}

            if aura_ds:
                Neo4jQueryRunner._configure_aura(config)

            driver = neo4j.GraphDatabase.driver(endpoint, auth=auth, **config)

            query_runner = Neo4jQueryRunner(
                driver,
                auto_close=True,
                bookmarks=bookmarks,
                config=config,
                database=database,
                show_progress=show_progress,
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
    def create_for_session(
        endpoint: str,
        auth: Optional[tuple[str, str]] = None,
        show_progress: bool = True,
    ) -> Neo4jQueryRunner:
        driver_config: dict[str, Any] = {"user_agent": f"neo4j-graphdatascience-v{__version__}"}

        Neo4jQueryRunner._configure_aura(driver_config)

        driver = neo4j.GraphDatabase.driver(endpoint, auth=auth, **driver_config)

        query_runner = Neo4jQueryRunner(
            driver,
            auto_close=True,
            show_progress=show_progress,
            bookmarks=None,
            config=driver_config,
            database="neo4j",
            instance_description="GDS Session",
        )

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
    def _configure_aura(config: dict[str, Any]) -> None:
        config["max_connection_lifetime"] = 60 * 8  # 8 minutes
        config["keep_alive"] = True
        config["max_connection_pool_size"] = 50

    def __init__(
        self,
        driver: neo4j.Driver,
        config: dict[str, Any] = {},
        database: Optional[str] = neo4j.DEFAULT_DATABASE,
        auto_close: bool = False,
        bookmarks: Optional[Any] = None,
        show_progress: bool = True,
        instance_description: str = "Neo4j DBMS",
    ):
        self._driver = driver
        self._config = config
        self._auto_close = auto_close
        self._database = database
        self._logger = logging.getLogger()
        self._bookmarks = bookmarks
        self._last_bookmarks: Optional[Any] = None
        self._server_version: Optional[ServerVersion] = None
        self._show_progress = show_progress
        self._progress_logger = QueryProgressLogger(
            self.__run_cypher_simplified_for_query_progress_logger, self.server_version
        )
        self._instance_description = instance_description

    def __run_cypher_simplified_for_query_progress_logger(self, query: str, database: Optional[str]) -> DataFrame:
        # progress logging should not retry a lot as it perodically fetches the latest progress anyway
        connectivity_retry_config = Neo4jQueryRunner.ConnectivityRetriesConfig(max_retries=2)
        return self.run_cypher(query=query, database=database, connectivity_retry_config=connectivity_retry_config)

    def run_cypher(
        self,
        query: str,
        params: Optional[dict[str, Any]] = None,
        database: Optional[str] = None,
        custom_error: bool = True,
        connectivity_retry_config: Optional[ConnectivityRetriesConfig] = None,
    ) -> DataFrame:
        if params is None:
            params = {}

        if database is None:
            database = self._database

        if connectivity_retry_config is None:
            connectivity_retry_config = Neo4jQueryRunner.ConnectivityRetriesConfig()
        self._verify_connectivity(database=database, retry_config=connectivity_retry_config)

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
        yields: Optional[list[str]] = None,
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

        job_id = None if not params else params.get_job_id()
        if self._resolve_show_progress(logging) and job_id:
            return self._progress_logger.run_with_progress_logging(run_cypher_query, job_id, database)
        else:
            return run_cypher_query()

    def _resolve_show_progress(self, show_progress: bool) -> bool:
        return self._show_progress and show_progress

    def server_version(self) -> ServerVersion:
        if self._server_version:
            return self._server_version

        try:
            server_version_string = self.run_cypher("RETURN gds.version()", custom_error=False).squeeze()
            server_version = ServerVersion.from_string(server_version_string)
            self._server_version = server_version
            return server_version
        except Exception as e:
            if "Unknown function 'gds.version'" in str(e):
                # Some Python versions appear to not call __del__ of self._query_runner when an exception
                # is raised, so we have to close the driver manually.
                self._driver.close()
                # if isinstance(endpoint, str):
                #    driver.close()

                raise GdsNotFound(
                    f"""The Graph Data Science library is not correctly installed on the {self._instance_description}.
                    Please refer to https://neo4j.com/docs/graph-data-science/current/installation/.
                    """
                )

            raise UnableToConnectError(e)

    def encrypted(self) -> bool:
        return self._driver.encrypted

    def driver_config(self) -> dict[str, Any]:
        return self._config

    def _forward_cypher_warnings(self, notification: dict[str, Any]) -> None:
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
        self, graph_name: str, concurrency: int, undirected_relationship_types: Optional[list[str]]
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

    def verify_connectivity(self) -> None:
        self._driver.verify_connectivity()

    def verify_authentication(self) -> None:
        self._driver.verify_authentication()

    def _verify_connectivity(
        self, database: Optional[str], retry_config: Neo4jQueryRunner.ConnectivityRetriesConfig
    ) -> None:
        # TODO allow for optional func to call (check session status on failure)
        if database is None:
            database = self._database

        exception = None
        retrys = 0
        while retrys < retry_config.max_retries:
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
                if retrys % retry_config.warn_interval == 0:
                    self._logger.warning(f"Unable to connect to the {self._instance_description}. Trying again...")

                time.sleep(retry_config.wait_time)
                retrys += 1

                continue

        if retrys == retry_config.max_retries:
            raise UnableToConnectError(f"Unable to connect to the {self._instance_description}") from exception

    class ConnectivityRetriesConfig(NamedTuple):
        max_retries: int = 600
        wait_time: int = 1
        warn_interval: int = 10
