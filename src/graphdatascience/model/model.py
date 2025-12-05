from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame, Series

from ..call_parameters import CallParameters
from ..graph.graph_object import Graph
from ..graph.graph_type_check import graph_type_check
from ..query_runner.query_runner import QueryRunner
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion


class Model(ABC):
    def __init__(self, name: str, query_runner: QueryRunner, server_version: ServerVersion):
        self._name = name
        self._query_runner = query_runner
        self._server_version = server_version

    @abstractmethod
    def _endpoint_prefix(self) -> str:
        pass

    def _list_info(self) -> Series[Any]:
        params = CallParameters(name=self.name())

        result: Series[Any]
        if self._server_version < ServerVersion(2, 5, 0):
            result = self._query_runner.call_procedure(
                "gds.beta.model.list", params=params, custom_error=False
            ).squeeze()
        else:
            result = self._query_runner.call_procedure("gds.model.list", params=params, custom_error=False).squeeze()

            if not result.empty:
                #  2.5 compat format
                result["modelInfo"] = {
                    **result["modelInfo"],
                    "modelName": result["modelName"],
                    "modelType": result["modelType"],
                }
                result["shared"] = result["published"]

        if result.empty:
            raise ValueError(f"There is no '{self.name()}' in the model catalog")

        return result

    def _estimate_predict(self, predict_mode: str, graph_name: str, config: dict[str, Any]) -> Series[Any]:
        endpoint = f"{self._endpoint_prefix()}{predict_mode}.estimate"
        config["modelName"] = self.name()
        params = CallParameters(graph_name=graph_name, config=config)

        return self._query_runner.call_procedure(  # type: ignore
            endpoint=endpoint,
            params=params,
        ).squeeze()

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
        return self._list_info()["modelInfo"]["modelType"]  # type: ignore

    def train_config(self) -> Series[Any]:
        """
        Get the train config of the model.

        Returns:
            The train config of the model.

        """
        train_config: Series[Any] = Series(self._list_info()["trainConfig"])
        return train_config

    def graph_schema(self) -> Series[Any]:
        """
        Get the graph schema of the model.

        Returns:
            The graph schema of the model.

        """
        graph_schema: Series[Any] = Series(self._list_info()["graphSchema"])
        return graph_schema

    def loaded(self) -> bool:
        """
        Check whether the model is loaded in memory.

        Returns:
            True if the model is loaded in memory, False otherwise.

        """
        return self._list_info()["loaded"]  # type: ignore

    def stored(self) -> bool:
        """
        Check whether the model is stored on disk.

        Returns:
            True if the model is stored on disk, False otherwise.

        """
        return self._list_info()["stored"]  # type: ignore

    def creation_time(self) -> Any:  # neo4j.time.DateTime not exported
        """
        Get the creation time of the model.

        Returns:
            The creation time of the model.

        """
        return self._list_info()["creationTime"]

    def shared(self) -> bool:
        """
        Check whether the model is shared.

        Returns:
            True if the model is shared, False otherwise.

        """
        return self._list_info()["shared"]  # type: ignore

    @compatible_with("published", min_inclusive=ServerVersion(2, 5, 0))
    def published(self) -> bool:
        """
        Check whether the model is published.

        Returns:
            True if the model is published, False otherwise.

        """
        return self._list_info()["published"]  # type: ignore

    def model_info(self) -> dict[str, Any]:
        """
        Get the model info of the model.

        Returns:
            The model info of the model.

        """
        return Series(self._list_info()["modelInfo"])  # type: ignore

    def exists(self) -> bool:
        """
        Check whether the model exists.

        Returns:
            True if the model exists, False otherwise.

        """
        name_space = "beta." if self._server_version < ServerVersion(2, 5, 0) else ""
        endpoint = f"gds.{name_space}model.exists"
        yields = ["exists"]

        params = CallParameters(model_name=self._name)

        return self._query_runner.call_procedure(  # type: ignore
            endpoint=endpoint, params=params, yields=yields, custom_error=False
        ).squeeze()

    def drop(self, failIfMissing: bool = False) -> Series[Any]:
        """
        Drop the model.

        Args:
            failIfMissing: If True, an error is thrown if the model does not exist. If False, no error is thrown.

        Returns:
            The result of the drop operation.

        """
        params = CallParameters(model_name=self._name, fail_if_missing=failIfMissing)
        if self._server_version < ServerVersion(2, 5, 0):
            return self._query_runner.call_procedure(  # type: ignore
                "gds.beta.model.drop", params=params, custom_error=False
            ).squeeze()
        else:
            result: Series[Any] = self._query_runner.call_procedure(
                "gds.model.drop", params=params, custom_error=False
            ).squeeze()

            if result.empty:
                return result

            #  modelInfo {.*, modelName: modelName, modelType: modelType} AS modelInfo
            result["modelInfo"] = {
                **result["modelInfo"],
                "modelName": result["modelName"],
                "modelType": result["modelType"],
            }
            result["shared"] = result["published"]
            return result

    def metrics(self) -> Series[Any]:
        """
        Get the metrics of the model.

        Returns:
            The metrics of the model.

        """
        model_info = self._list_info()["modelInfo"]
        metrics: Series[Any] = Series(model_info["metrics"])
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
        endpoint = f"{self._endpoint_prefix()}stream"
        config["modelName"] = self.name()
        params = CallParameters(graph_name=G.name(), config=config)

        return self._query_runner.call_procedure(endpoint=endpoint, params=params, logging=True)

    @graph_type_check
    def predict_stream_estimate(self, G: Graph, **config: Any) -> Series[Any]:
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
    def predict_mutate(self, G: Graph, **config: Any) -> Series[Any]:
        """
        Predict on the given graph using the model and mutate the graph with the results.

        Args:
            G: The graph to predict on.
            **config: The config for the prediction.

        Returns:
            The result of mutate operation.

        """
        endpoint = f"{self._endpoint_prefix()}mutate"
        config["modelName"] = self.name()
        params = CallParameters(graph_name=G.name(), config=config)

        return self._query_runner.call_procedure(  # type: ignore
            endpoint=endpoint, params=params, logging=True
        ).squeeze()

    @graph_type_check
    def predict_mutate_estimate(self, G: Graph, **config: Any) -> Series[Any]:
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
