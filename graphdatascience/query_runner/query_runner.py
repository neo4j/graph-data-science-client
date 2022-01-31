from abc import ABC, abstractmethod
from typing import Any, Dict, List

SingleRow = Dict[str, Any]
QueryResult = List[SingleRow]


class QueryRunner(ABC):
    @abstractmethod
    def run_query(self, query: str, params: Dict[str, Any] = {}) -> QueryResult:
        pass

    @abstractmethod
    def set_database(self, db: str) -> None:
        pass
