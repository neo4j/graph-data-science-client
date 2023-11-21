from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from pandas import DataFrame

from ..call_parameters import CallParameters
from ..server_version.server_version import ServerVersion
from .graph_constructor import GraphConstructor


class QueryRunner(ABC):
    @abstractmethod
    def call_procedure(
        self,
        endpoint: str,
        params: Optional[CallParameters] = None,
        yields: Optional[List[str]] = None,
        database: Optional[str] = None,
        logging: bool = False,
        custom_error: bool = True,
    ) -> DataFrame:
        pass

    @abstractmethod
    def run_cypher(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
        custom_error: bool = True,
    ) -> DataFrame:
        pass

    @abstractmethod
    def server_version(self) -> ServerVersion:
        pass

    @abstractmethod
    def driver_config(self) -> Dict[str, Any]:
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
        self, graph_name: str, concurrency: int, undirected_relationship_types: Optional[List[str]]
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

    def set_server_version(self, _: ServerVersion) -> None:
        pass
