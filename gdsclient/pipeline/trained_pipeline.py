from abc import ABC, abstractmethod
from typing import Any

from ..graph.graph_object import Graph
from ..query_runner.query_runner import QueryResult, QueryRunner


class TrainedPipeline(ABC):
    def __init__(self, name: str, query_runner: QueryRunner) -> None:
        self._name = name
        self._query_runner = query_runner

    @abstractmethod
    def _query_prefix(self) -> str:
        pass

    def name(self) -> str:
        return self._name

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
