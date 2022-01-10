from abc import ABC, abstractmethod
from typing import Any, Dict

from ..graph.graph_object import Graph
from ..query_runner.query_runner import QueryResult, QueryRunner
from .model import Model


class PredictionPipeline(Model, ABC):
    def __init__(self, name: str, query_runner: QueryRunner) -> None:
        super().__init__(name, query_runner)

    @abstractmethod
    def _query_prefix(self) -> str:
        pass

    def metrics(self) -> Dict[str, Dict[str, Any]]:
        return self._list_info()["modelInfo"]["metrics"]  # type: ignore

    def predict_stream(self, G: Graph, **config: Any) -> QueryResult:
        query = f"{self._query_prefix()}stream($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query(query, params)

    def predict_mutate(self, G: Graph, **config: Any) -> QueryResult:
        query = f"{self._query_prefix()}mutate($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query(query, params)
