from abc import ABC, abstractmethod
from typing import Any, Dict

from pandas.core.frame import DataFrame

from ..server_version.server_version import ServerVersion
from .graph_constructor import GraphConstructor


class QueryRunner(ABC):
    @abstractmethod
    def run_query(self, query: str, params: Dict[str, Any] = {}) -> DataFrame:
        pass

    def run_query_with_logging(self, query: str, params: Dict[str, Any] = {}) -> DataFrame:
        return self.run_query(query, params)

    @abstractmethod
    def set_database(self, db: str) -> None:
        pass

    def close(self) -> None:
        pass

    @abstractmethod
    def create_graph_constructor(self, graph_name: str, concurrency: int) -> GraphConstructor:
        pass

    @abstractmethod
    def database(self) -> str:
        pass

    def set_server_version(self, _: ServerVersion) -> None:
        pass
