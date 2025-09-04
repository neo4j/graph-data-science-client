from __future__ import annotations

import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List, Optional, Union

import neo4j
from pydantic import Field, field_validator

from graphdatascience import Graph
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.node_label_endpoints import NodeLabelEndpoints
from graphdatascience.procedure_surface.api.graph_sampling_endpoints import GraphSamplingEndpoints


class CatalogEndpoints(ABC):
    @abstractmethod
    def list(self, G: Optional[Union[Graph, str]] = None) -> List[GraphListResult]:
        """List graphs in the graph catalog.

        Args:
            G (Optional[Union[Graph, str]], optional): Graph object or name to filter results.
               If None, list all graphs. Defaults to None.

        Returns:
            List[GraphListResult]: List of graph metadata objects containing information like
                                 graph name, node count, relationship count, etc.
        """
        pass

    @abstractmethod
    def drop(self, G: Union[Graph, str], fail_if_missing: Optional[bool] = None) -> Optional[GraphListResult]:
        """Drop a graph from the graph catalog.

        Args:
            G (Union[Graph, str]): Graph object or name to drop.
            fail_if_missing (Optional[bool], optional): Whether to fail if the graph is missing. Defaults to None.

        Returns:
              GraphListResult: Graph metadata object containing information like
                               graph name, node count, relationship count, etc.
        """

    @abstractmethod
    def filter(
        self,
        G: Graph,
        graph_name: str,
        node_filter: str,
        relationship_filter: str,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
    ) -> GraphFilterResult:
        """Create a subgraph of a graph based on a filter expression.

        Parameters
        ----------
        G (Graph):
            Graph object to filter on
        graph_name (str):
            Name of subgraph to create
        node_filter (str):
            Filter expression for nodes
        relationship_filter (str):
            Filter expression for relationships
        concurrency (int, optional):
            Number of concurrent threads to use. Defaults to None.
        job_id (str, optional):
            Unique identifier for the filtering job. Defaults to None.

        Returns:
            GraphFilterResult: Filter result containing information like
                                graph name, node count, relationship count, etc.
        """
        pass

    @abstractmethod
    def generate(
        self,
        graph_name: str,
        node_count: int,
        average_degree: float,
        *,
        relationship_distribution: Optional[str] = None,
        relationship_seed: Optional[int] = None,
        relationship_property: Optional[RelationshipPropertySpec] = None,
        orientation: Optional[str] = None,
        allow_self_loops: Optional[bool] = None,
        read_concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
    ) -> GraphGenerationStats:
        """
        Generates a random graph and store it in the graph catalog.

        Parameters
        ----------
        graph_name : str
            Name of the generated graph.
        node_count : int
            The number of nodes in the generated graph
        average_degree : float
            The average out-degree of the generated nodes
        relationship_distribution : Optional[str], default=None
            Determines the relationship distribution strategy.
        relationship_seed : Optional[int], default=None
            Seed value for generating deterministic relationships.
        relationship_property : Optional[RelationshipPropertySpec], default=None
            Configure generated relationship properties.
        orientation : Optional[str], default=None
            Specifies the orientation of the generated relationships.
        allow_self_loops : Optional[bool], default=None
            Whether nodes in the graph can have relationships where start and end nodes are the same.
        read_concurrency : Optional[int], default=None
            Number of concurrent threads/processes to use during graph generation.
        job_id : Optional[str], default=None
            Unique identifier for the job associated with the graph generation.
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress during graph generation.
        username : Optional[str], default=None
            Username of the individual requesting the graph generation.

        Returns
        -------
        GraphGenerationStats:
            A result object containing information about the generated graph.
        """

    @property
    @abstractmethod
    def sample(self) -> GraphSamplingEndpoints:
        """Endpoints for graph sampling."""
        pass

    @property
    @abstractmethod
    def node_labels(self) -> NodeLabelEndpoints:
        """Endpoints for node label operations."""
        pass

class GraphListResult(BaseResult):
    graph_name: str
    database: str
    database_location: str
    configuration: dict[str, Any]
    memory_usage: str
    size_in_bytes: int
    node_count: int
    relationship_count: int
    creation_time: datetime
    modification_time: datetime
    graph_schema: dict[str, Any] = Field(alias="schema")
    schema_with_orientation: dict[str, Any]
    degree_distribution: Optional[dict[str, Any]] = None

    @field_validator("creation_time", "modification_time", mode="before")
    @classmethod
    def strip_timezone(cls, value: Any) -> Any:
        if isinstance(value, str):
            return re.sub(r"\[.*\]$", "", value)
        if isinstance(value, neo4j.time.DateTime):
            return value.to_native()
        return value


class GraphFilterResult(BaseResult):
    graph_name: str
    from_graph_name: str
    node_filter: str
    relationship_filter: str
    node_count: int
    relationship_count: int
    project_millis: int


class GraphGenerationStats(BaseResult):
    name: str
    nodes: int
    relationships: int
    generate_millis: int
    relationship_seed: Optional[int]
    average_degree: float
    relationship_distribution: str
    relationship_property: RelationshipPropertySpec


class RelationshipPropertySpec(BaseResult):
    name: str
    type: str
    min: Optional[float] = None
    max: Optional[float] = None
    value: Optional[float] = None

    @staticmethod
    def fixed(name: str, value: float) -> RelationshipPropertySpec:
        return RelationshipPropertySpec(name=name, type="FIXED", value=value)

    @staticmethod
    def random(name: str, min: float, max: float) -> RelationshipPropertySpec:
        return RelationshipPropertySpec(name=name, type="RANDOM", min=min, max=max)
