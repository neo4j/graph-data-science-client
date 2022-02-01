from abc import ABC, abstractmethod
from typing import Any, Dict, List

Row = Dict[str, Any]
QueryResult = List[Row]


class QueryRunner(ABC):
    @abstractmethod
    def run_query(self, query: str, params: Dict[str, Any] = {}) -> QueryResult:
        pass

    @abstractmethod
    def set_database(self, db: str) -> None:
        pass
