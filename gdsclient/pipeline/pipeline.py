from typing import Any, Dict, List
from abc import ABC, abstractmethod

from ..graph.graph_object import Graph
from ..query_runner.query_runner import QueryRunner
from .lp_trained_pipeline import LPTrainedPipeline


class Pipeline(ABC):
    def __init__(self, name: str, query_runner: QueryRunner):
        self._name = name
        self._query_runner = query_runner

    @abstractmethod
    def _query_prefix(self):
        pass

    @abstractmethod
    def _create_trained_model(self, name: str, query_runner: QueryRunner) -> Any:
        pass

    def _pipeline_info(self) -> Dict[str, Any]:
        query = "CALL gds.beta.model.list($name)"
        params = {"name": self.name()}

        return self._query_runner.run_query(query, params)[0]["modelInfo"]  # type: ignore

    def name(self) -> str:
        return self._name

    def addNodeProperty(self, procedure_name: str, **config: Any) -> None:
        query = f"{self._query_prefix()}addNodeProperty($pipeline_name, $procedure_name, $config)"
        params = {
            "pipeline_name": self.name(),
            "procedure_name": procedure_name,
            "config": config,
        }
        self._query_runner.run_query(query, params)

    def configureParams(self, parameter_space: List[Dict[str, Any]]) -> None:
        query = (
            f"{self._query_prefix()}configureParams($pipeline_name, $parameter_space)"
        )
        params = {
            "pipeline_name": self.name(),
            "parameter_space": parameter_space,
        }
        self._query_runner.run_query(query, params)

    # TODO: do we want to log the train result or return Pair(Result, Model)
    def train(self, G: Graph, **config: Any) -> LPTrainedPipeline:
        query = f"{self._query_prefix()}train($graph_name, $config)"
        config["pipeline"] = self.name()
        params = {
            "graph_name": G.name(),
            "config": config,
        }

        self._query_runner.run_query(query, params)

        return self._create_trained_model(config["modelName"], self._query_runner)

    def configureSplit(self, **config: Any) -> None:
        query = f"{self._query_prefix()}configureSplit($pipeline_name, $config)"
        params = {"pipeline_name": self.name(), "config": config}
        self._query_runner.run_query(query, params)

    def node_property_steps(self) -> List[Dict[str, Any]]:
        return self._pipeline_info()["featurePipeline"]["nodePropertySteps"]  # type: ignore

    def split_config(self) -> Dict[str, Any]:
        return self._pipeline_info()["splitConfig"]  # type: ignore

    def parameter_space(self) -> List[Dict[str, Any]]:
        return self._pipeline_info()["trainingParameterSpace"]  # type: ignore
