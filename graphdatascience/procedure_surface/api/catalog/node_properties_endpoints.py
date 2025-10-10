from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2


class NodePropertiesEndpoints(ABC):
    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        node_properties: str | list[str],
        *,
        list_node_labels: bool | None = None,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        job_id: Any | None = None,
        db_node_properties: list[str] | None = None,
    ) -> DataFrame:
        """
        Streams the specified node properties from the graph.

        Parameters
        ----------
        G : GraphV2
            The graph to stream node properties from
        node_properties : str | list[str]
            The node properties to stream
        list_node_labels : boolean | None, default=None
            Whether to include node labels in the stream
        node_labels : list[str] | None, default=None
            Filter by node labels
        concurrency : Any | None, default=None
            The number of concurrent threads
        sudo : bool | None, default=None
            Override memory estimation limits
        log_progress : bool | None, default=None
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        job_id : Any | None, default=None
            An identifier for the job
        db_node_properties : list[str] | None, default=None
            Retrieves additional node properties from the database and attaches them to the stream.
        Returns
        -------
        DataFrame
            The streamed node properties
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        node_properties: str | list[str] | dict[str, str],
        *,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
        write_concurrency: Any | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        job_id: Any | None = None,
    ) -> NodePropertiesWriteResult:
        """
        Writes the specified node properties from the graph to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to write node properties from
        node_properties : str | list[str] | dict[str, str]
            The node properties to write.
            If a dictionary is provided, the keys are the property names and the values are the aliases that will be used as the property name in the database.
        node_labels : list[str] | None, default=None
            Filter by node labels
        concurrency : Any | None, default=None
            The number of concurrent threads
        write_concurrency : Any | None, default=None
            The number of concurrent threads used for writing
        sudo : bool | None, default=None
            Override memory estimation limits
        log_progress : bool | None, default=None
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        job_id : Any | None, default=None
            An identifier for the job
        Returns
        -------
        NodePropertiesWriteResult
            Execution metrics and statistics
        """
        pass

    @abstractmethod
    def drop(
        self,
        G: GraphV2,
        node_properties: list[str],
        *,
        fail_if_missing: bool | None = None,
        concurrency: Any | None = None,
        username: str | None = None,
    ) -> NodePropertiesDropResult:
        """
        Drops the specified node properties from the graph.

        Parameters
        ----------
        G : GraphV2
            The graph to drop node properties from
        node_properties : list[str]
            The node properties to drop
        fail_if_missing: bool | None = None,
            Whether to fail if any of the node properties are missing
        concurrency : Any | None, default=None
            The number of concurrent threads
        username : str | None, default=None
            The username to attribute the procedure run to
        Returns
        -------
        NodePropertiesDropResult
            Execution metrics and statistics
        """
        pass


@dataclass
class NodePropertySpec:
    def __init__(self, node_properties: str | list[str] | dict[str, str]) -> None:
        match node_properties:
            case str():
                self._mappings = {node_properties: node_properties}
            case list():
                self._mappings = {prop: prop for prop in node_properties}
            case dict():
                self._mappings = node_properties

    def property_names(self) -> list[str]:
        return list(self._mappings.keys())

    def to_dict(self) -> dict[str, str]:
        return self._mappings.copy()


class NodePropertiesWriteResult(BaseResult):
    graph_name: str
    node_properties: list[str]
    properties_written: int
    write_millis: int
    configuration: dict[str, Any]


class NodePropertiesDropResult(BaseResult):
    graph_name: str
    node_properties: list[str]
    properties_removed: int
