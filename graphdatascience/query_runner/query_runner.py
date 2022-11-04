from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from pandas import DataFrame

from ..server_version.server_version import ServerVersion
from .graph_constructor import GraphConstructor


class QueryRunner(ABC):
    @abstractmethod
    def run_procedure_endpoint(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None, database: Optional[str] = None
    ) -> DataFrame:
        pass

    def run_procedure_endpoint_with_logging(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None, database: Optional[str] = None
    ) -> DataFrame:
        return self.run_procedure_endpoint(endpoint, params, database)

    @abstractmethod
    def run_function_endpoint(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None, database: Optional[str] = None
    ) -> DataFrame:
        pass

    @abstractmethod
    def set_database(self, database: str) -> None:
        pass

    def close(self) -> None:
        pass

    @abstractmethod
    def create_graph_constructor(self, graph_name: str, concurrency: int) -> GraphConstructor:
        pass

    @abstractmethod
    def database(self) -> Optional[str]:
        pass

    def set_server_version(self, _: ServerVersion) -> None:
        pass
