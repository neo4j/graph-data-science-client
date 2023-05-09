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

        info = self._query_runner.run_query(query, params, custom_error=False)

        if len(info) == 0:
            raise ValueError(f"There is no '{self.name()}' in the model catalog")

        return info

    def _estimate_predict(self, predict_mode: str, graph_name: str, config: Dict[str, Any]) -> "Series[Any]":
        query = f"{self._query_prefix()}{predict_mode}.estimate($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": graph_name, "config": config}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def name(self) -> str:
        """
        Get the name of the model.

        Returns:
            The name of the model.

        """
        return self._name

    def type(self) -> str:
        """
        Get the type of the model.

        Returns:
            The type of the model.

        """
        return self._list_info()["modelInfo"][0]["modelType"]  # type: ignore

    def train_config(self) -> "Series[Any]":
        """
        Get the train config of the model.

        Returns:
            The train config of the model.

        """
        train_config: "Series[Any]" = Series(self._list_info()["trainConfig"][0])
        return train_config

    def graph_schema(self) -> "Series[Any]":
        """
        Get the graph schema of the model.

        Returns:
            The graph schema of the model.

        """
        graph_schema: "Series[Any]" = Series(self._list_info()["graphSchema"][0])
        return graph_schema

    def loaded(self) -> bool:
        """
        Check whether the model is loaded in memory.

        Returns:
            True if the model is loaded in memory, False otherwise.

        """
        return self._list_info()["loaded"].squeeze()  # type: ignore

    def stored(self) -> bool:
        """
        Check whether the model is stored on disk.

        Returns:
            True if the model is stored on disk, False otherwise.

        """
        return self._list_info()["stored"].squeeze()  # type: ignore

    def creation_time(self) -> Any:  # neo4j.time.DateTime not exported
        """
        Get the creation time of the model.

        Returns:
            The creation time of the model.

        """
        return self._list_info()["creationTime"].squeeze()

    def shared(self) -> bool:
        """
        Check whether the model is shared.

        Returns:
            True if the model is shared, False otherwise.

        """
        return self._list_info()["shared"].squeeze()  # type: ignore

    def model_info(self) -> "Series[Any]":
        """
        Get the model info of the model.

        Returns:
            The model info of the model.

        """
        return Series(self._list_info()["modelInfo"].squeeze())

    def exists(self) -> bool:
        """
        Check whether the model exists.

        Returns:
            True if the model exists, False otherwise.

        """
        query = "CALL gds.beta.model.exists($model_name) YIELD exists"
        params = {"model_name": self._name}

        return self._query_runner.run_query(query, params, custom_error=False).squeeze()  # type: ignore

    def drop(self, failIfMissing: bool = False) -> "Series[Any]":
        """
        Drop the model.

        Args:
            failIfMissing: If True, an error is thrown if the model does not exist. If False, no error is thrown.

        Returns:
            The result of the drop operation.

        """
        query = "CALL gds.beta.model.drop($model_name, $fail_if_missing)"
        params = {"model_name": self._name, "fail_if_missing": failIfMissing}

        return self._query_runner.run_query(query, params, custom_error=False).squeeze()  # type: ignore

    def metrics(self) -> "Series[Any]":
        """
        Get the metrics of the model.

        Returns:
            The metrics of the model.

        """
        model_info = self._list_info()["modelInfo"][0]
        metrics: "Series[Any]" = Series(model_info["metrics"])
        return metrics

    @graph_type_check
    def predict_stream(self, G: Graph, **config: Any) -> DataFrame:
        """
        Predict on the given graph using the model and stream the results as DataFrame

        Args:
            G: The graph to predict on.
            **config: The config for the prediction.

        Returns:
            The prediction results as DataFrame.

        """
        query = f"{self._query_prefix()}stream($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query_with_logging(query, params)

    @graph_type_check
    def predict_stream_estimate(self, G: Graph, **config: Any) -> "Series[Any]":
        """
        Estimate the prediction on the given graph using the model and stream the results as DataFrame

        Args:
            G: The graph to predict on.
            **config: The config for the prediction.

        Returns:
            The prediction results as DataFrame.

        """
        return self._estimate_predict("stream", G.name(), config)

    @graph_type_check
    def predict_mutate(self, G: Graph, **config: Any) -> "Series[Any]":
        """
        Predict on the given graph using the model and mutate the graph with the results.

        Args:
            G: The graph to predict on.
            **config: The config for the prediction.

        Returns:
            The result of mutate operation.

        """
        query = f"{self._query_prefix()}mutate($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query_with_logging(query, params).squeeze()  # type: ignore

    @graph_type_check
    def predict_mutate_estimate(self, G: Graph, **config: Any) -> "Series[Any]":
        """
        Estimate the memory needed to predict on the given graph using the model.

        Args:
            G: The graph to predict on.
            **config: The config for the prediction.

        Returns:
            The memory needed to predict on the given graph using the model.

        """
        return self._estimate_predict("mutate", G.name(), config)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name()}, type={self.type()})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._list_info().to_dict()})"
