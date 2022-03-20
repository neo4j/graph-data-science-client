from abc import ABC
from typing import Any, Dict, Tuple

from pandas.core.frame import DataFrame
from pandas.core.series import Series

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..graph.graph_object import Graph
from ..model.graphsage_model import GraphSageModel
from ..query_runner.query_runner import QueryRunner


class AlgoProcRunner(IllegalAttrChecker, ABC):
    def __init__(self, query_runner: QueryRunner, proc_name: str):
        self._query_runner = query_runner
        self._proc_name = proc_name

    def _run_procedure(self, G: Graph, config: Dict[str, Any]) -> DataFrame:
        query = f"CALL {self._proc_name}($graph_name, $config)"

        params: Dict[str, Any] = {}
        params["graph_name"] = G.name()
        params["config"] = config

        return self._query_runner.run_query(query, params)

    def estimate(self, G: Graph, **config: Any) -> Series:
        self._proc_name += "." + "estimate"
        return self._run_procedure(G, config).squeeze()  # type: ignore


class StreamModeRunner(AlgoProcRunner):
    def __call__(self, G: Graph, **config: Any) -> DataFrame:
        return self._run_procedure(G, config)


class StandardModeRunner(AlgoProcRunner):
    def __call__(self, G: Graph, **config: Any) -> Series:
        return self._run_procedure(G, config).squeeze()  # type: ignore


class GraphSageRunner(AlgoProcRunner):
    def __call__(self, G: Graph, **config: Any) -> Tuple[GraphSageModel, Series]:
        result = self._run_procedure(G, config).squeeze()
        model_name = result["modelInfo"]["modelName"]

        return GraphSageModel(model_name, self._query_runner), result
