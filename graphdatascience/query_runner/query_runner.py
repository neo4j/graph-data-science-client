from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from neo4j import Bookmarks
from pandas import DataFrame

from ..server_version.server_version import ServerVersion
from .graph_constructor import GraphConstructor


class QueryRunner(ABC):
    @abstractmethod
    def run_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
        custom_error: bool = True,
    ) -> DataFrame:
        pass

    def run_query_with_logging(
        self, query: str, params: Optional[Dict[str, Any]] = None, database: Optional[str] = None
    ) -> DataFrame:
        return self.run_query(query, params, database, True)

    @abstractmethod
    def set_database(self, database: str) -> None:
        pass

    @abstractmethod
    def set_bookmarks(self, bookmarks: Optional[Bookmarks]) -> None:
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
    def bookmarks(self) -> Optional[Bookmarks]:
        pass

    @abstractmethod
    def last_bookmarks(self) -> Optional[Bookmarks]:
        pass

    def set_server_version(self, _: ServerVersion) -> None:
        pass
