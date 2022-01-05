from typing import Any

from ..graph.graph_object import Graph
from ..query_runner.query_runner import QueryResult, QueryRunner


class LPTrainedPipeline:
    _PREDICT_QUERY_PREFIX = "CALL gds.alpha.ml.pipeline.linkPrediction.predict."

    def __init__(self, name: str, query_runner: QueryRunner) -> None:
        self._name = name
        self._query_runner = query_runner

    def name(self) -> str:
        return self._name

    def predict_stream(self, G: Graph, **config: Any) -> QueryResult:
        query = f"{self._PREDICT_QUERY_PREFIX}stream($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query(query, params)

    def predict_mutate(self, G: Graph, **config: Any) -> QueryResult:
        query = f"{self._PREDICT_QUERY_PREFIX}mutate($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query(query, params)
