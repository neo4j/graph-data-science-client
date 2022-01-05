from typing import Any, Dict, List

from ..graph.graph_object import Graph
from ..query_runner.query_runner import QueryResult, QueryRunner


class LPPipeline:
    _QUERY_PREFIX = "CALL gds.alpha.ml.pipeline.linkPrediction."

    def __init__(self, name: str, query_runner: QueryRunner):
        self._name = name
        self._query_runner = query_runner

    def _pipeline_info(self) -> Dict[str, Any]:
        query = "CALL gds.beta.model.list($name)"
        params = {"name": self.name()}

        return self._query_runner.run_query(query, params)[0]["modelInfo"]  # type: ignore

    def name(self) -> str:
        return self._name

    def addNodeProperty(self, procedure_name: str, **config: Any) -> None:
        query = f"{self._QUERY_PREFIX}addNodeProperty($pipeline_name, $procedure_name, $config)"
        params = {
            "pipeline_name": self.name(),
            "procedure_name": procedure_name,
            "config": config,
        }
        self._query_runner.run_query(query, params)

    def addFeature(self, feature_type: str, **config: Any) -> None:
        query = (
            f"{self._QUERY_PREFIX}addFeature($pipeline_name, $feature_type, $config)"
        )
        params = {
            "pipeline_name": self.name(),
            "feature_type": feature_type,
            "config": config,
        }
        self._query_runner.run_query(query, params)

    def configureParams(self, parameter_space: List[Dict[str, Any]]) -> None:
        query = f"{self._QUERY_PREFIX}configureParams($pipeline_name, $parameter_space)"
        params = {
            "pipeline_name": self.name(),
            "parameter_space": parameter_space,
        }
        self._query_runner.run_query(query, params)

    def train(self, G: Graph, **config: Any) -> QueryResult:
        query = f"{self._QUERY_PREFIX}train($graph_name, $config)"
        config["pipeline"] = self.name()
        params = {
            "graph_name": G.name(),
            "config": config,
        }
        return self._query_runner.run_query(query, params)

    def configureSplit(self, **config: Any) -> None:
        query = f"{self._QUERY_PREFIX}configureSplit($pipeline_name, $config)"
        params = {"pipeline_name": self.name(), "config": config}
        self._query_runner.run_query(query, params)

    def node_property_steps(self) -> List[Dict[str, Any]]:
        return self._pipeline_info()["featurePipeline"]["nodePropertySteps"]  # type: ignore

    def feature_steps(self) -> List[Dict[str, Any]]:
        return self._pipeline_info()["featurePipeline"]["featureSteps"]  # type: ignore

    def split_config(self) -> Dict[str, Any]:
        return self._pipeline_info()["splitConfig"]  # type: ignore

    def parameter_space(self) -> List[Dict[str, Any]]:
        return self._pipeline_info()["trainingParameterSpace"]  # type: ignore
