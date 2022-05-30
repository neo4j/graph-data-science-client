from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple

import pandas
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from ..graph.graph_object import Graph
from ..model.model import Model
from ..query_runner.query_runner import QueryRunner
from ..server_version.server_version import ServerVersion


class TrainingPipeline(ABC):
    def __init__(self, name: str, query_runner: QueryRunner, server_version: ServerVersion):
        self._name = name
        self._query_runner = query_runner
        self._server_version = server_version

    def name(self) -> str:
        return self._name

    @abstractmethod
    def _query_prefix(self) -> str:
        pass

    @abstractmethod
    def _create_trained_model(self, name: str, query_runner: QueryRunner) -> Model:
        pass

    def addNodeProperty(self, procedure_name: str, **config: Any) -> Series:
        query = f"{self._query_prefix()}addNodeProperty($pipeline_name, $procedure_name, $config)"
        params = {
            "pipeline_name": self.name(),
            "procedure_name": procedure_name,
            "config": config,
        }
        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    @staticmethod
    def _expand_ranges(config: Dict[str, Any]) -> Dict[str, Any]:
        def _maybe_expand_tuple(value: Any) -> Any:
            return {"range": list(value)} if isinstance(value, tuple) else value

        return {key: _maybe_expand_tuple(val) for (key, val) in config.items()}

    def configureAutoTuning(self, **config: Any) -> Series:
        query_prefix = self._query_prefix().replace("beta", "alpha")
        query = f"{query_prefix}configureAutoTuning($pipeline_name, $config)"
        params = {"pipeline_name": self.name(), "config": config}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def train(self, G: Graph, **config: Any) -> Tuple[Model, Series]:
        query = f"{self._query_prefix()}train($graph_name, $config)"
        config["pipeline"] = self.name()
        params = {
            "graph_name": G.name(),
            "config": config,
        }

        result = self._query_runner.run_query_with_logging(query, params).squeeze()

        return (
            self._create_trained_model(config["modelName"], self._query_runner),
            result,
        )

    def train_estimate(self, G: Graph, **config: Any) -> Series:
        query = f"{self._query_prefix()}train.estimate($graph_name, $config)"
        config["pipeline"] = self.name()
        params = {
            "graph_name": G.name(),
            "config": config,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def configureSplit(self, **config: Any) -> Series:
        query = f"{self._query_prefix()}configureSplit($pipeline_name, $config)"
        params = {"pipeline_name": self.name(), "config": config}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def node_property_steps(self) -> DataFrame:
        pipeline_info = self._list_info()["pipelineInfo"][0]
        return pandas.DataFrame(pipeline_info["featurePipeline"]["nodePropertySteps"])

    def split_config(self) -> Series:
        pipeline_info = self._list_info()["pipelineInfo"][0]
        return pandas.Series(pipeline_info["splitConfig"])

    def parameter_space(self) -> Series:
        pipeline_info = self._list_info()["pipelineInfo"][0]
        return pandas.Series(pipeline_info["trainingParameterSpace"])

    def auto_tuning_config(self) -> Series:
        pipeline_info = self._list_info()["pipelineInfo"][0]
        return pandas.Series(pipeline_info["autoTuningConfig"])

    def _list_info(self) -> DataFrame:
        query = "CALL gds.beta.pipeline.list($name)"
        params = {"name": self.name()}

        info = self._query_runner.run_query(query, params)

        if len(info) == 0:
            raise ValueError(f"There is no '{self.name()}' in the pipeline catalog")

        return info

    def type(self) -> str:
        return self._list_info()["pipelineType"].squeeze()  # type: ignore

    def creation_time(self) -> Any:  # neo4j.time.DateTime not exported
        return self._list_info()["creationTime"].squeeze()

    def exists(self) -> bool:
        query = "CALL gds.beta.pipeline.exists($pipeline_name) YIELD exists"
        params = {"pipeline_name": self._name}

        return self._query_runner.run_query(query, params)["exists"].squeeze()  # type: ignore

    def drop(self, failIfMissing: bool = False) -> Series:
        query = "CALL gds.beta.pipeline.drop($pipeline_name, $fail_if_missing)"
        params = {"pipeline_name": self._name, "fail_if_missing": failIfMissing}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore
