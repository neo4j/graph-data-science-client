from typing import Any

from pandas import Series

from ..graph.graph_object import Graph
from ..graph.graph_type_check import graph_type_check
from .model import Model


class GraphSageModel(Model):
    def _query_prefix(self) -> str:
        return "CALL gds.beta.graphSage."

    @graph_type_check
    def predict_write(self, G: Graph, **config: Any) -> "Series[Any]":
        query = f"{self._query_prefix()}write($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query_with_logging(query, params).squeeze()  # type: ignore

    @graph_type_check
    def predict_write_estimate(self, G: Graph, **config: Any) -> "Series[Any]":
        return self._estimate_predict("write", G.name(), config)
