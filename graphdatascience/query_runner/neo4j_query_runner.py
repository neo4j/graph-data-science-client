from __future__ import annotations

import logging
import re
import time
import warnings
from typing import Any, NamedTuple, Optional, Union

import neo4j
from pandas import DataFrame

from graphdatascience.query_runner.query_mode import QueryMode

from ..call_parameters import CallParameters
from ..error.endpoint_suggester import generate_suggestive_error_message
from ..error.gds_not_installed import GdsNotFound
from ..error.unable_to_connect import UnableToConnectError
from ..semantic_version.semantic_version import SemanticVersion
from ..server_version.server_version import ServerVersion
from ..version import __version__
from .cypher_graph_constructor import CypherGraphConstructor
from .graph_constructor import GraphConstructor
from .progress.query_progress_logger import QueryProgressLogger
from .query_runner import QueryRunner


class Neo4jQueryRunner(QueryRunner):
    _AURA_DS_PROTOCOL = "neo4j+s"
    _LOG_POLLING_INTERVAL = 0.5
    _NEO4J_DRIVER_VERSION = SemanticVersion.from_string(neo4j.__version__)

    @staticmethod
    def create_for_db(
        endpoint: Union[str, neo4j.Driver],
        auth: Union[tuple[str, str], neo4j.Auth, None] = None,
        aura_ds: bool = False,
        database: Optional[str] = None,
        bookmarks: Optional[Any] = None,
        show_progress: bool = True,
        config: Optional[dict[str, Any]] = None,
    ) -> Neo4jQueryRunner:
        if isinstance(endpoint, str):
            if config is None:
                config = {}

            config["user_agent"] = f"neo4j-graphdatascience-v{__version__}"

            if aura_ds:
                Neo4jQueryRunner._configure_aura(config)

            driver = neo4j.GraphDatabase.driver(endpoint, auth=auth, **config)

            query_runner = Neo4jQueryRunner(
                driver,
                Neo4jQueryRunner.parse_protocol(endpoint),
                auth,
                auto_close=True,
                bookmarks=bookmarks,
                config=config,
                database=database,
                show_progress=show_progress,
            )

        elif isinstance(endpoint, neo4j.Driver):
            protocol = "neo4j+s" if endpoint.encrypted else "bolt"
            query_runner = Neo4jQueryRunner(
                endpoint,
                protocol,
                auto_close=False,
                bookmarks=bookmarks,
                database=database,
                show_progress=show_progress,
            )
        else:
            raise ValueError(f"Invalid endpoint type: {type(endpoint)}")

        return query_runner

    @staticmethod
    def create_for_session(
        endpoint: str,
        auth: Union[tuple[str, str], neo4j.Auth, None] = None,
        show_progress: bool = True,
    ) -> Neo4jQueryRunner:
        driver_config: dict[str, Any] = {"user_agent": f"neo4j-graphdatascience-v{__version__}"}

        Neo4jQueryRunner._configure_aura(driver_config)

        driver = neo4j.GraphDatabase.driver(endpoint, auth=auth, **driver_config)

        query_runner = Neo4jQueryRunner(
            driver,
            Neo4jQueryRunner.parse_protocol(endpoint),
            auth,
            auto_close=True,
            show_progress=show_progress,
            bookmarks=None,
            config=driver_config,
            # we need to explicitly set this as the default value is None
            # database in the session is always neo4j
            database="neo4j",
            instance_description="GDS Session",
        )

        return query_runner

    @staticmethod
    def _configure_aura(config: dict[str, Any]) -> None:
        # defaults as documented in https://support.neo4j.com/s/article/1500001173021-How-to-handle-Session-Expired-Errors-while-connecting-to-Neo4j-Aura
        config.setdefault("max_connection_lifetime", 60 * 50)  # 50 minutes
        config.setdefault("keep_alive", True)
        config.setdefault("max_connection_pool_size", 50)

        if Neo4jQueryRunner._NEO4J_DRIVER_VERSION >= SemanticVersion(5, 16, 0):
            config.setdefault("liveness_check_timeout", 60 * 5)  # 5 minutes

    @staticmethod
    def parse_protocol(endpoint: str) -> str:
        protocol_match = re.match(r"^([^:]+)://", endpoint)
        if not protocol_match:
            raise ValueError(f"Invalid endpoint URI format: {endpoint}")
        return protocol_match.group(1)

    def __init__(
        self,
        driver: neo4j.Driver,
        protocol: str,
        auth: Union[tuple[str, str], neo4j.Auth, None] = None,
        config: dict[str, Any] = {},
        database: Optional[str] = neo4j.DEFAULT_DATABASE,
        auto_close: bool = False,
        bookmarks: Optional[Any] = None,
        show_progress: bool = True,
        instance_description: str = "Neo4j DBMS",
    ):
        self._driver = driver
        self._protocol = protocol
        self._auth = auth
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
        # not using retryable cypher as failing is okay
        return self.run_cypher(query=query, database=database, connectivity_retry_config=connectivity_retry_config)

    # only use for user defined queries
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

            self.__configure_warnings_filter()

            df = result.to_df()

            if self._NEO4J_DRIVER_VERSION < SemanticVersion(5, 0, 0):
                self._last_bookmarks = [session.last_bookmark()]
            else:
                self._last_bookmarks = session.last_bookmarks()

            notifications = result.consume().notifications
            if notifications:
                for notification in notifications:
                    self._forward_cypher_warnings(notification)

            return df

    # better retry mechanism than run_cypher. The neo4j driver handles retryable errors internally
    def run_retryable_cypher(
        self,
        query: str,
        params: Optional[dict[str, Any]] = None,
        database: Optional[str] = None,
        custom_error: bool = True,
        mode: Optional[QueryMode] = None,
        connectivity_retry_config: Optional[ConnectivityRetriesConfig] = None,
    ) -> DataFrame:
        if not database:
            database = self._database

        if self._NEO4J_DRIVER_VERSION < SemanticVersion(5, 5, 0):
            return self.run_cypher(query, params, database, custom_error, connectivity_retry_config)

        if not mode:
            routing = neo4j.RoutingControl.READ
        else:
            routing = mode.neo4j_routing()

        try:
            return self._driver.execute_query(
                query_=query,
                parameters_=params,
                database_=database,
                result_transformer_=neo4j.Result.to_df,
                bookmark_manager_=self.bookmarks(),
                routing_=routing,
            )
        except Exception as e:
            if custom_error:
                Neo4jQueryRunner.handle_driver_exception(self._driver, e)
                raise e
            else:
                raise e

    def call_function(self, endpoint: str, params: Optional[CallParameters] = None, custom_error: bool = True) -> Any:
        if params is None:
            params = CallParameters()
        query = f"RETURN {endpoint}({params.placeholder_str()})"

        # we can use retryable cypher as we expect all gds functions to be idempotent
        return self.run_retryable_cypher(query, params, custom_error=custom_error, mode=QueryMode.READ).squeeze()

    def call_procedure(
        self,
        endpoint: str,
        params: Optional[CallParameters] = None,
        yields: Optional[list[str]] = None,
        database: Optional[str] = None,
        mode: QueryMode = QueryMode.READ,
        logging: bool = False,
        retryable: bool = False,
        custom_error: bool = True,
    ) -> DataFrame:
        if params is None:
            params = CallParameters()

        yields_clause = "" if yields is None else " YIELD " + ", ".join(yields)
        query = f"CALL {endpoint}({params.placeholder_str()}){yields_clause}"

        def run_cypher_query() -> DataFrame:
            if retryable:
                return self.run_retryable_cypher(query, params, database, custom_error, mode=mode)
            else:
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
            server_version_string = self.call_function("gds.version", custom_error=False)
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
            if "deprecated field" in notification["description"] and "procedure" in notification["description"]:
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

    def cloneWithoutRouting(self, host: str, port: int) -> QueryRunner:
        protocol = self._protocol.replace("neo4j", "bolt")
        endpoint = "{}://{}:{}".format(protocol, host, port)
        driver = neo4j.GraphDatabase.driver(endpoint, auth=self._auth, **self.driver_config())

        return Neo4jQueryRunner(
            driver=driver,
            protocol=protocol,
            auth=self._auth,
            config=self._config,
            database=self._database,
            auto_close=self._auto_close,
            bookmarks=self._bookmarks,
            show_progress=self._show_progress,
            instance_description=self._instance_description,
        )

    @staticmethod
    def handle_driver_exception(cypher_executor: Union[neo4j.Session, neo4j.Driver], e: Exception) -> None:
        reg_gds_hit = re.search(
            r"There is no procedure with the name `(gds(?:\.\w+)+)` registered for this database instance",
            str(e),
        )
        if not reg_gds_hit:
            raise e

        requested_endpoint = reg_gds_hit.group(1)

        if isinstance(cypher_executor, neo4j.Session):
            list_result = cypher_executor.run("CALL gds.list() YIELD name")
            all_endpoints = list_result.to_df()["name"].tolist()
        elif isinstance(cypher_executor, neo4j.Driver):
            result = cypher_executor.execute_query("CALL gds.list() YIELD name", result_transformer_=neo4j.Result.to_df)
            all_endpoints = result["name"].tolist()
        else:
            raise TypeError(
                f"Expected cypher_executor to be a neo4j.Session or neo4j.Driver, got {type(cypher_executor)}"
            )

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
                if self._NEO4J_DRIVER_VERSION < SemanticVersion(5, 0, 0):
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

    def __configure_warnings_filter(self) -> None:
        if Neo4jQueryRunner._NEO4J_DRIVER_VERSION >= SemanticVersion(5, 21, 0):
            notifications_logger = logging.getLogger("neo4j.notifications")
            # the client does not expose YIELD fields so we just skip these warnings for now
            notifications_logger.addFilter(
                lambda record: (
                    "The query used a deprecated field from a procedure" in record.msg and "by 'gds." in record.msg
                )
            )
            notifications_logger.addFilter(
                lambda record: "The procedure has a deprecated field" in record.msg and "gds." in record.msg
            )
        warnings.filterwarnings(
            "ignore",
            message=r"^pandas support is experimental and might be changed or removed in future versions$",
        )
        # neo4j 2025.04
        warnings.filterwarnings("ignore", message=r".*The procedure has a deprecated field.*by 'gds.*")
        # neo4j driver 4.4
        warnings.filterwarnings("ignore", message=r".*The query used a deprecated field from a procedure.*by 'gds.*")

    class ConnectivityRetriesConfig(NamedTuple):
        max_retries: int = 600
        wait_time: int = 1
        warn_interval: int = 10
