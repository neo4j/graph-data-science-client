from abc import ABC, abstractmethod
from typing import Any, Dict

from pandas import DataFrame, Series

from ..graph.graph_object import Graph
from ..graph.graph_type_check import graph_type_check
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

    def _estimate_predict(self, predict_mode: str, graph_name: str, config: Dict[str, Any]) -> "Series[Any]":
        query = f"{self._query_prefix()}{predict_mode}.estimate($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": graph_name, "config": config}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def name(self) -> str:
        return self._name

    def type(self) -> str:
        return self._list_info()["modelInfo"][0]["modelType"]  # type: ignore

    def train_config(self) -> "Series[Any]":
        train_config: "Series[Any]" = Series(self._list_info()["trainConfig"][0])
        return train_config

    def graph_schema(self) -> "Series[Any]":
        graph_schema: "Series[Any]" = Series(self._list_info()["graphSchema"][0])
        return graph_schema

    def loaded(self) -> bool:
        return self._list_info()["loaded"].squeeze()  # type: ignore

    def stored(self) -> bool:
        return self._list_info()["stored"].squeeze()  # type: ignore

    def creation_time(self) -> Any:  # neo4j.time.DateTime not exported
        return self._list_info()["creationTime"].squeeze()

    def shared(self) -> bool:
        return self._list_info()["shared"].squeeze()  # type: ignore

    def model_info(self) -> "Series[Any]":
        return Series(self._list_info()["modelInfo"].squeeze())

    def exists(self) -> bool:
        query = "CALL gds.beta.model.exists($model_name) YIELD exists"
        params = {"model_name": self._name}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def drop(self, failIfMissing: bool = False) -> "Series[Any]":
        query = "CALL gds.beta.model.drop($model_name, $fail_if_missing)"
        params = {"model_name": self._name, "fail_if_missing": failIfMissing}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def metrics(self) -> "Series[Any]":
        model_info = self._list_info()["modelInfo"][0]
        metrics: "Series[Any]" = Series(model_info["metrics"])
        return metrics

    @graph_type_check
    def predict_stream(self, G: Graph, **config: Any) -> DataFrame:
        query = f"{self._query_prefix()}stream($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query_with_logging(query, params)

    @graph_type_check
    def predict_stream_estimate(self, G: Graph, **config: Any) -> "Series[Any]":
        return self._estimate_predict("stream", G.name(), config)

    @graph_type_check
    def predict_mutate(self, G: Graph, **config: Any) -> "Series[Any]":
        query = f"{self._query_prefix()}mutate($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query_with_logging(query, params).squeeze()  # type: ignore

    @graph_type_check
    def predict_mutate_estimate(self, G: Graph, **config: Any) -> "Series[Any]":
        return self._estimate_predict("mutate", G.name(), config)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name()}, type={self.type()})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._list_info().to_dict()})"
