from abc import ABC, abstractmethod
from typing import Any, Optional

from pandas import DataFrame

from graphdatascience.query_runner.query_mode import QueryMode

from ..call_parameters import CallParameters
from ..server_version.server_version import ServerVersion
from .graph_constructor import GraphConstructor


class QueryRunner(ABC):
    @abstractmethod
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
        pass

    @abstractmethod
    def call_function(self, endpoint: str, params: Optional[CallParameters] = None) -> Any:
        pass

    @abstractmethod
    def run_cypher(
        self,
        query: str,
        params: Optional[dict[str, Any]] = None,
        database: Optional[str] = None,
        custom_error: bool = True,
    ) -> DataFrame:
        pass

    @abstractmethod
    def run_retryable_cypher(
        self,
        query: str,
        params: Optional[dict[str, Any]] = None,
        database: Optional[str] = None,
        custom_error: bool = True,
    ) -> DataFrame:
        pass

    @abstractmethod
    def server_version(self) -> ServerVersion:
        pass

    @abstractmethod
    def driver_config(self) -> dict[str, Any]:
        pass

    @abstractmethod
    def encrypted(self) -> bool:
        pass

    @abstractmethod
    def set_database(self, database: str) -> None:
        pass

    @abstractmethod
    def set_bookmarks(self, bookmarks: Optional[Any]) -> None:
        pass

    def close(self) -> None:
        pass

    @abstractmethod
    def create_graph_constructor(
        self, graph_name: str, concurrency: int, undirected_relationship_types: Optional[list[str]]
    ) -> GraphConstructor:
        pass

    @abstractmethod
    def database(self) -> Optional[str]:
        pass

    @abstractmethod
    def bookmarks(self) -> Optional[Any]:
        pass

    @abstractmethod
    def last_bookmarks(self) -> Optional[Any]:
        pass

    @abstractmethod
    def set_show_progress(self, show_progress: bool) -> None:
        pass

    @abstractmethod
    def cloneWithoutRouting(self, host: str, port: int) -> "QueryRunner":
        pass

    def set_server_version(self, _: ServerVersion) -> None:
        pass
