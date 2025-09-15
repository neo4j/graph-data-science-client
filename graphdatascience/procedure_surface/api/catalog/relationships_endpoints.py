from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience import Graph
from graphdatascience.procedure_surface.api.base_result import BaseResult


class RelationshipsEndpoints(ABC):
    @abstractmethod
    def stream(
        self,
        G: Graph,
        relationship_types: Optional[List[str]] = None,
        *,
        concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> DataFrame:
        """
        Streams the specified relationships from the graph.

        Parameters
        ----------
        G : Graph
            The graph to stream relationships from
        relationship_types: Optional[List[str]], default = None
            The relationship types to stream
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
            The streamed relationships
        """
        pass

    @abstractmethod
    def write(
        self,
        G: Graph,
        relationship_type: str,
        *,
        concurrency: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> RelationshipsWriteResult:
        """
        Writes the specified relationships from the graph to the database.

        Parameters
        ----------
        G : Graph
            The graph to write relationships from
        relationship_type : str
            The relationship type to write to the database
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
        RelationshipsWriteResult
            Execution metrics and statistics
        """
        pass

    @abstractmethod
    def drop(
        self,
        G: Graph,
        relationship_type: str,
    ) -> RelationshipsDropResult:
        """
        Drops the specified relationships from the graph.

        Parameters
        ----------
        G : Graph
            The graph to drop relationships from
        relationship_type: str
            The relationship type to drop
        Returns
        -------
        RelationshipsDropResult
            Execution metrics and statistics
        """
        pass

    @abstractmethod
    def index_inverse(
        self,
        G: Graph,
        relationship_types: list[str],
        *,
        concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> RelationshipsInverseIndexResult:
        """
        Creates an index of the specified relationships indexing the reverse direction of each relationship.
        This index can be used by some algorithm to speed up the computation.

        Parameters
        ----------
        G : Graph
            The graph to operate on
        relationship_types: Optional[List[str]] = None,
            The relationship types to create the inverse index for
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
        RelationshipsInverseIndexResult
            Execution metrics and statistics
        """

    @abstractmethod
    def to_undirected(
        self,
        G: Graph,
        relationship_type: str,
        mutate_relationship_type: str,
        aggregation: Optional[Union[Aggregation, dict[str, Aggregation]]] = None,
        *,
        concurrency: Optional[Any] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        job_id: Optional[Any] = None,
    ) -> RelationshipsToUndirectedResult:
        """
        Creates a new relationship type in the graph.
        The relationship will be based on an existing relationship type, however, the relationships will be stored undirected.

        Parameters
        ----------
        G : Graph
            The graph to operate on
        relationship_type: str,
            The input relationship type
        mutate_relationship_type: str,
            The name of the undirected relationship type to create
        aggregation: Optional[Union[Aggregation, dict[str, Aggregation]]] = None,
            Specifies how to aggregate parallel relationships in the graph.
            If a single aggregation is provided, it will be used for properties of the specified relationships.
            A dictionary can be provided to specify property specific aggregations.
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
        RelationshipsInverseIndexResult
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


class RelationshipsWriteResult(BaseResult):
    graph_name: str
    relationship_type: str
    relationships_written: int
    write_millis: int
    configuration: dict[str, Any]


class RelationshipsDropResult(BaseResult):
    graph_name: str
    relationship_type: str
    deleted_relationships: int
    deleted_properties: dict[str, int]


class RelationshipsInverseIndexResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    post_processing_millis: int
    input_relationships: int
    configuration: dict[str, Any]


class RelationshipsToUndirectedResult(RelationshipsInverseIndexResult):
    relationships_written: int


class Aggregation(Enum):
    NONE = "none"
    SINGLE = "single"
    SUM = "sum"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
