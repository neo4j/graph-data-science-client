from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from pandas import DataFrame, Series

from graphdatascience.model.v2.model_info import ModelInfo

from ..call_parameters import CallParameters
from ..graph.graph_object import Graph
from ..graph.graph_type_check import graph_type_check
from ..query_runner.query_runner import QueryRunner
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion


class InfoProvider(ABC):
    @abstractmethod
    def fetch(self, model_name: str) -> ModelInfo:
        """Return the task with progress for the given job_id."""
        pass


class Model(ABC):
    def __init__(self, name: str, info_provider: InfoProvider):
        self._name = name
        self._info_provider = info_provider

    # TODO estimate mode, predict modes on here?
    # implement Cypher and Arrow info_provider and stuff

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
        return self._info_provider.fetch(self._name).type

    def train_config(self) -> Series[Any]:
        """
        Get the train config of the model.

        Returns:
            The train config of the model.

        """
        return self._info_provider.fetch(self._name).train_config

    def graph_schema(self) -> Series[Any]:
        """
        Get the graph schema of the model.

        Returns:
            The graph schema of the model.

        """
        return self._info_provider.fetch(self._name).graph_schema

    def loaded(self) -> bool:
        """
        Check whether the model is loaded in memory.

        Returns:
            True if the model is loaded in memory, False otherwise.

        """
        return self._info_provider.fetch(self._name).loaded

    def stored(self) -> bool:
        """
        Check whether the model is stored on disk.

        Returns:
            True if the model is stored on disk, False otherwise.

        """
        return self._info_provider.fetch(self._name).stored

    def creation_time(self) -> datetime.datetime:
        """
        Get the creation time of the model.

        Returns:
            The creation time of the model.

        """
        return self._info_provider.fetch(self._name).creation_time

    def shared(self) -> bool:
        """
        Check whether the model is shared.

        Returns:
            True if the model is shared, False otherwise.

        """
        return self._info_provider.fetch(self._name).shared

    def published(self) -> bool:
        """
        Check whether the model is published.

        Returns:
            True if the model is published, False otherwise.

        """
        return self._info_provider.fetch(self._name).published

    def model_info(self) -> dict[str, Any]:
        """
        Get the model info of the model.

        Returns:
            The model info of the model.

        """
        return self._info_provider.fetch(self._name).model_info

    def exists(self) -> bool:
        """
        Check whether the model exists.

        Returns:
            True if the model exists, False otherwise.

        """
        raise NotImplementedError()

    def drop(self, failIfMissing: bool = False) -> Series[Any]:
        """
        Drop the model.

        Args:
            failIfMissing: If True, an error is thrown if the model does not exist. If False, no error is thrown.

        Returns:
            The result of the drop operation.

        """
        raise NotImplementedError()

    def metrics(self) -> Series[Any]:
        """
        Get the metrics of the model.

        Returns:
            The metrics of the model.

        """
        model_info = self._info_provider.fetch(self._name).model_info
        metrics: Series[Any] = Series(model_info["metrics"])
        return metrics

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name()}, type={self.type()})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._info_provider.fetch(self._name).to_dict()})"
