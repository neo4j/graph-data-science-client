from abc import ABC, abstractmethod
from typing import Any, Dict

import pandas
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from ..graph.graph_object import Graph
from ..query_runner.query_runner import QueryRunner
from ..server_version.server_version import ServerVersion


class Model(ABC):
    def __init__(self, name: str, query_runner: QueryRunner, server_version: ServerVersion):
        self._name = name
        self._query_runner = query_runner
        self._server_version = server_version

    @abstractmethod
    def _query_prefix(self) -> str:
        pass

    def _list_info(self) -> DataFrame:
        query = "CALL gds.beta.model.list($name)"
        params = {"name": self.name()}

        info = self._query_runner.run_query(query, params)

        if len(info) == 0:
            raise ValueError(f"There is no '{self.name()}' in the model catalog")

        return info

    def _estimate_predict(self, predict_mode: str, graph_name: str, config: Dict[str, Any]) -> Series:
        query = f"{self._query_prefix()}{predict_mode}.estimate($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": graph_name, "config": config}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def name(self) -> str:
        return self._name

    def type(self) -> str:
        return self._list_info()["modelInfo"][0]["modelType"]  # type: ignore

    def train_config(self) -> Series:
        return pandas.Series(self._list_info()["trainConfig"][0])

    def graph_schema(self) -> Series:
        return pandas.Series(self._list_info()["graphSchema"][0])

    def loaded(self) -> bool:
        return self._list_info()["loaded"].squeeze()  # type: ignore

    def stored(self) -> bool:
        return self._list_info()["stored"].squeeze()  # type: ignore

    def creation_time(self) -> Any:  # neo4j.time.DateTime not exported
        return self._list_info()["creationTime"].squeeze()

    def shared(self) -> bool:
        return self._list_info()["shared"].squeeze()  # type: ignore

    def exists(self) -> bool:
        query = "CALL gds.beta.model.exists($model_name) YIELD exists"
        params = {"model_name": self._name}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def drop(self, failIfMissing: bool = False) -> Series:
        query = "CALL gds.beta.model.drop($model_name, $fail_if_missing)"
        params = {"model_name": self._name, "fail_if_missing": failIfMissing}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def metrics(self) -> Series:
        model_info = self._list_info()["modelInfo"][0]
        return pandas.Series(model_info["metrics"])

    def predict_stream(self, G: Graph, **config: Any) -> DataFrame:
        query = f"{self._query_prefix()}stream($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query_with_logging(query, params)

    def predict_stream_estimate(self, G: Graph, **config: Any) -> Series:
        return self._estimate_predict("stream", G.name(), config)

    def predict_mutate(self, G: Graph, **config: Any) -> Series:
        query = f"{self._query_prefix()}mutate($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query_with_logging(query, params).squeeze()  # type: ignore

    def predict_mutate_estimate(self, G: Graph, **config: Any) -> Series:
        return self._estimate_predict("mutate", G.name(), config)
