from abc import ABC, abstractmethod
from typing import Any, Dict, List

from pandas.core.frame import DataFrame

Row = Dict[str, Any]
QueryResult = List[Row]


class QueryRunner(ABC):
    @abstractmethod
    def run_query(self, query: str, params: Dict[str, Any] = {}) -> DataFrame:
        pass

    @abstractmethod
    def set_database(self, db: str) -> None:
        pass
