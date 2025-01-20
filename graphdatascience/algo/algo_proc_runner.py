from abc import ABC
from typing import Any

from pandas import DataFrame, Series

from ..call_parameters import CallParameters
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..graph.graph_object import Graph
from ..graph.graph_type_check import graph_type_check
from ..model.graphsage_model import GraphSageModel


class AlgoProcRunner(IllegalAttrChecker, ABC):
    @graph_type_check
    def _run_procedure(self, G: Graph, config: dict[str, Any], with_logging: bool = True) -> DataFrame:
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint=self._namespace, params=params, logging=with_logging)

    @graph_type_check
    def estimate(self, G: Graph, **config: Any) -> "Series[Any]":
        self._namespace += "." + "estimate"
        return self._run_procedure(G, config, with_logging=False).squeeze()  # type: ignore


class StreamModeRunner(AlgoProcRunner):
    def __call__(self, G: Graph, **config: Any) -> DataFrame:
        return self._run_procedure(G, config)


class StandardModeRunner(AlgoProcRunner):
    def __call__(self, G: Graph, **config: Any) -> "Series[Any]":
        return self._run_procedure(G, config).squeeze()  # type: ignore


class GraphSageRunner(AlgoProcRunner):
    @graph_type_check
    def __call__(self, G: Graph, **config: Any) -> tuple[GraphSageModel, "Series[Any]"]:
        result = self._run_procedure(G, config).squeeze()
        model_name = result["modelInfo"]["modelName"]

        return GraphSageModel(model_name, self._query_runner, self._server_version), result
