from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS


class NodePropertiesEndpoints(ABC):
    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        node_properties: str | list[str],
        *,
        list_node_labels: bool | None = None,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        job_id: str | None = None,
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
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency
            Number of concurrent threads to use.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        job_id
            Identifier for the computation.
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
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        write_concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        job_id: str | None = None,
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
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency
            Number of concurrent threads to use.
        write_concurrency
            Number of concurrent threads to use for writing.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        job_id
            Identifier for the computation.
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
        concurrency: int | None = None,
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
        concurrency
            Number of concurrent threads to use.
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
