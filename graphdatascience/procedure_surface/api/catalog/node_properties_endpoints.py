from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2


class NodePropertiesEndpoints(ABC):
    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        node_properties: Union[str, List[str]],
        *,
        list_node_labels: Optional[bool] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
        db_node_properties: Optional[List[str]] = None,
    ) -> DataFrame:
        """
        Streams the specified node properties from the graph.

        Parameters
        ----------
        G : Graph
            The graph to stream node properties from
        node_properties : Union[str, List[str]]
            The node properties to stream
        list_node_labels : Optional[boolean], default=None
            Whether to include node labels in the stream
        node_labels : Optional[List[str]], default=None
            Filter by node labels
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        job_id : Optional[Any], default=None
            An identifier for the job
        db_node_properties : Optional[List[str]], default=None
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
        node_properties: Union[str, List[str], dict[str, str]],
        *,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> NodePropertiesWriteResult:
        """
        Writes the specified node properties from the graph to the database.

        Parameters
        ----------
        G : Graph
            The graph to write node properties from
        node_properties : Union[str, List[str], dict[str, str]]
            The node properties to write.
            If a dictionary is provided, the keys are the property names and the values are the aliases that will be used as the property name in the database.
        node_labels : Optional[List[str]], default=None
            Filter by node labels
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        write_concurrency : Optional[Any], default=None
            The number of concurrent threads used for writing
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        job_id : Optional[Any], default=None
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
        node_properties: List[str],
        *,
        fail_if_missing: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> NodePropertiesDropResult:
        """
        Drops the specified node properties from the graph.

        Parameters
        ----------
        G : Graph
            The graph to drop node properties from
        node_properties : List[str]
            The node properties to drop
        fail_if_missing: Optional[bool] = None,
            Whether to fail if any of the node properties are missing
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        job_id : Optional[Any], default=None
            An identifier for the job
        Returns
        -------
        NodePropertiesDropResult
            Execution metrics and statistics
        """
        pass


@dataclass
class NodePropertySpec:
    def __init__(self, node_properties: Union[str, List[str], dict[str, str]]) -> None:
        if isinstance(node_properties, str):
            self._mappings = {node_properties: node_properties}
        elif isinstance(node_properties, list):
            self._mappings = {prop: prop for prop in node_properties}
        elif isinstance(node_properties, dict):
            self._mappings = node_properties

    def property_names(self) -> List[str]:
        return list(self._mappings.keys())

    def to_dict(self) -> dict[str, str]:
        return self._mappings.copy()


class NodePropertiesWriteResult(BaseResult):
    graph_name: str
    node_properties: List[str]
    properties_written: int
    write_millis: int
    configuration: dict[str, Any]


class NodePropertiesDropResult(BaseResult):
    graph_name: str
    node_properties: List[str]
    properties_removed: int
