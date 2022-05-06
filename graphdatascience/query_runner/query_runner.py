from abc import ABC, abstractmethod
from typing import Any, Dict

from pandas.core.frame import DataFrame

from .graph_constructor import GraphConstructor


class QueryRunner(ABC):
    @abstractmethod
    def run_query(self, query: str, params: Dict[str, Any] = {}) -> DataFrame:
        pass

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
