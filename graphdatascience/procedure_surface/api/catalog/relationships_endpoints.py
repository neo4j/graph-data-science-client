from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

from pandas import DataFrame
from pydantic import AliasChoices, Field, field_validator

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_TYPES


class RelationshipsEndpoints(ABC):
    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        relationship_types: list[str] = ALL_TYPES,
        relationship_properties: list[str] | None = None,
        *,
        concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
    ) -> DataFrame:
        """
        Streams all relationships of the specified types with the specified properties.

        Parameters
        ----------
        G : Graph
            The graph to stream relationships from
        relationship_types: list[str] | None, default = None
            The relationship types to stream
            If not specified, all relationships in the graph will be streamed.
        relationship_properties: list[str] | None, default = None
            The relationship properties to stream. If not specified, no properties will be streamed.
        concurrency : int | None, default=None
            The number of concurrent threads
        sudo : bool
            Override memory estimation limits
        log_progress : bool = True
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        Returns
        -------
        DataFrame
            The streamed relationships [sourceId, targetId, relationshipType] with a column for each property
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        relationship_type: str,
        relationship_properties: list[str] | None = None,
        *,
        concurrency: int | None = None,
        write_concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        job_id: str | None = None,
    ) -> RelationshipsWriteResult:
        """
        Writes all relationships of the specified relationship type with the specified properties from the graph to the database.

        Parameters
        ----------
        G : Graph
            The graph to write relationships from
        relationship_type : str
            The relationship type to write to the database
        relationship_properties: list[str] | None, default = None
            The relationship properties to write. If not specified, no properties will be written.
        concurrency : int | None, default=None
            The number of concurrent threads
        write_concurrency : int | None, default=None
            The number of concurrent threads used for writing
        sudo : bool
            Override memory estimation limits
        log_progress : bool | None, default=None
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        job_id : str | None, default=None
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
        G: GraphV2,
        relationship_type: str,
        *,
        fail_if_missing: bool = True,
    ) -> RelationshipsDropResult:
        """
        Drops all relationships of the specified relationship type, including all their properties, from the graph.

        Parameters
        ----------
        G : Graph
            The graph to drop relationships from
        relationship_type: str
            The relationship type to drop
        fail_if_missing: bool, default=True
            If set to true, the procedure will fail if the relationship type does not exist in the graph.
        Returns
        -------
        RelationshipsDropResult
            Execution metrics and statistics
        """
        pass

    @abstractmethod
    def index_inverse(
        self,
        G: GraphV2,
        relationship_types: list[str],
        *,
        concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        job_id: str | None = None,
    ) -> RelationshipsInverseIndexResult:
        """
        Creates an index of the specified relationships indexing the reverse direction of each relationship.
        This index can be used by some algorithm to speed up the computation.

        Parameters
        ----------
        G : Graph
            The graph to operate on
        relationship_types: list[str] = ALL_TYPES,
            The relationship types to create the inverse index for
        concurrency : int | None, default=None
            The number of concurrent threads
        sudo : bool = False,
            Override memory estimation limits
        log_progress : bool = True
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        job_id : str | None, default=None
            An identifier for the job
        Returns
        -------
        RelationshipsInverseIndexResult
            Execution metrics and statistics
        """

    @abstractmethod
    def to_undirected(
        self,
        G: GraphV2,
        relationship_type: str,
        mutate_relationship_type: str,
        *,
        aggregation: Aggregation | dict[str, Aggregation] | None = None,
        concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        job_id: str | None = None,
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
        aggregation: Aggregation | dict[str, Aggregation] | None = None,
            Specifies how to aggregate parallel relationships in the graph.
            If a single aggregation is provided, it will be used for properties of the specified relationships.
            A dictionary can be provided to specify property specific aggregations.
        concurrency : int | None, default=None
            The number of concurrent threads
        sudo : bool = False,
            Override memory estimation limits
        log_progress : bool = True
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        job_id : str | None, default=None
            An identifier for the job
        Returns
        -------
        RelationshipsInverseIndexResult
            Execution metrics and statistics
        """

    pass


class RelationshipsWriteResult(BaseResult):
    graph_name: str
    relationship_type: str
    relationship_properties: list[str] = Field(
        default=[], validation_alias=AliasChoices("relationshipProperty", "relationshipProperties")
    )
    relationships_written: int
    properties_written: int = 0
    write_millis: int
    configuration: dict[str, Any]

    @field_validator("relationship_properties", mode="before")
    @classmethod
    def coerce_relationship_properties(cls, v: Any) -> list[str]:
        if v is None:
            return []
        elif isinstance(v, str):
            return [v]
        else:
            return v  # type: ignore


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


class Aggregation(str, Enum):
    NONE = "NONE"
    SINGLE = "SINGLE"
    SUM = "SUM"
    MIN = "MIN"
    MAX = "MAX"
    COUNT = "COUNT"
