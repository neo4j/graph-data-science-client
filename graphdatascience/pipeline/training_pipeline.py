from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple

from ..graph.graph_object import Graph
from ..model.model import Model
from ..query_runner.query_runner import QueryRunner, Row


class TrainingPipeline(ABC):
    def __init__(self, name: str, query_runner: QueryRunner):
        self._name = name
        self._query_runner = query_runner

    def name(self) -> str:
        return self._name

    @abstractmethod
    def _query_prefix(self) -> str:
        pass

    @abstractmethod
    def _create_trained_model(self, name: str, query_runner: QueryRunner) -> Model:
        pass

    def addNodeProperty(self, procedure_name: str, **config: Any) -> Row:
        query = f"{self._query_prefix()}addNodeProperty($pipeline_name, $procedure_name, $config)"
        params = {
            "pipeline_name": self.name(),
            "procedure_name": procedure_name,
            "config": config,
        }
        return self._query_runner.run_query(query, params)[0]

    def configureParams(self, parameter_space: List[Dict[str, Any]]) -> Row:
        query = (
            f"{self._query_prefix()}configureParams($pipeline_name, $parameter_space)"
        )
        params = {
            "pipeline_name": self.name(),
            "parameter_space": parameter_space,
        }
        return self._query_runner.run_query(query, params)[0]

    def train(self, G: Graph, **config: Any) -> Tuple[Model, Row]:
        query = f"{self._query_prefix()}train($graph_name, $config)"
        config["pipeline"] = self.name()
        params = {
            "graph_name": G.name(),
            "config": config,
        }

        result = self._query_runner.run_query(query, params)[0]

        return (
            self._create_trained_model(config["modelName"], self._query_runner),
            result,
        )

    def configureSplit(self, **config: Any) -> Row:
        query = f"{self._query_prefix()}configureSplit($pipeline_name, $config)"
        params = {"pipeline_name": self.name(), "config": config}

        return self._query_runner.run_query(query, params)[0]

    def node_property_steps(self) -> List[Dict[str, Any]]:
        return self._list_info()["pipelineInfo"]["featurePipeline"]["nodePropertySteps"]  # type: ignore

    def split_config(self) -> Dict[str, Any]:
        return self._list_info()["pipelineInfo"]["splitConfig"]  # type: ignore

    def parameter_space(self) -> List[Dict[str, Any]]:
        return self._list_info()["pipelineInfo"]["trainingParameterSpace"]  # type: ignore

    def _list_info(self) -> Row:
        query = "CALL gds.beta.pipeline.list($name)"
        params = {"name": self.name()}

        info = self._query_runner.run_query(query, params)

        if len(info) == 0:
            raise ValueError(f"There is no '{self.name()}' in the pipeline catalog")

        return info[0]

    def type(self) -> str:
        return self._list_info()["pipelineType"]  # type: ignore

    def creation_time(self) -> Any:  # neo4j.time.DateTime not exported
        return self._list_info()["creationTime"]

    def exists(self) -> bool:
        query = "CALL gds.beta.pipeline.exists($pipeline_name) YIELD exists"
        params = {"pipeline_name": self._name}

        return self._query_runner.run_query(query, params)[0]["exists"]  # type: ignore

    def drop(self) -> None:
        query = "CALL gds.beta.pipeline.drop($pipeline_name)"
        params = {"pipeline_name": self._name}

        self._query_runner.run_query(query, params)
