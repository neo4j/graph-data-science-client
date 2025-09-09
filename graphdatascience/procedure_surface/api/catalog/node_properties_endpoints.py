from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional, Union

from pandas import DataFrame

from graphdatascience import Graph
from graphdatascience.procedure_surface.api.base_result import BaseResult


class NodePropertiesEndpoints(ABC):
    @abstractmethod
    def stream(
        self,
        G: Graph,
        node_properties: list[Union[str, NodePropertySpec]],
        *,
        list_node_labels: Optional[bool] = None,
        node_labels: Optional[list[str]] = None,
        concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> DataFrame:
        """
        Streams the specified node properties from the graph.

        Parameters
        ----------
        G : Graph
            The graph to stream node properties from
        node_properties : list[Union[str, NodePropertySpec]]
            The node properties to stream
        list_node_labels : Optional[boolean], default=None
            Whether to include node labels in the stream
        node_labels : Optional[list[str]], default=None
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
        Returns
        -------
        DataFrame
            The streamed node properties
        """
        pass

    @abstractmethod
    def write(
        self,
        G: Graph,
        node_properties: list[Union[str, NodePropertySpec]],
        *,
        node_labels: Optional[list[str]] = None,
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
        node_properties : list[Union[str, NodePropertySpec]]
            The node properties to stream
        node_labels : Optional[list[str]], default=None
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
        G: Graph,
        node_properties: list[str],
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
        node_properties : list[str]
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


class NodePropertySpec(BaseResult):
    name: str
    alias: Optional[str] = None

    def effective_name(self) -> str:
        return self.alias if self.alias is not None else self.name


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
