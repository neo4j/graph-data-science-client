from __future__ import annotations

from abc import ABC, abstractmethod
from types import TracebackType
from typing import List, NamedTuple, Optional, Type, Union

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.graph_info import GraphInfo, GraphInfoWithDegrees
from graphdatascience.procedure_surface.api.catalog.node_label_endpoints import NodeLabelEndpoints
from graphdatascience.procedure_surface.api.catalog.node_properties_endpoints import NodePropertiesEndpoints
from graphdatascience.procedure_surface.api.catalog.relationships_endpoints import RelationshipsEndpoints
from graphdatascience.procedure_surface.api.graph_sampling_endpoints import GraphSamplingEndpoints


class CatalogEndpoints(ABC):
    @abstractmethod
    def list(self, G: Optional[Union[GraphV2, str]] = None) -> List[GraphInfoWithDegrees]:
        """List graphs in the graph catalog.

        Args:
            G (Optional[Union[GraphV2, str]], optional): GraphV2 object or name to filter results.
               If None, list all graphs. Defaults to None.

        Returns:
            List[GraphListResult]: List of graph metadata objects containing information like
                                 graph name, node count, relationship count, etc.
        """
        pass

    @abstractmethod
    def drop(self, G: Union[GraphV2, str], fail_if_missing: bool = True) -> Optional[GraphInfo]:
        """Drop a graph from the graph catalog.

        Args:
            G (Union[GraphV2, str]): GraphV2 object or name to drop.
            fail_if_missing (bool): Whether to fail if the graph is missing. Defaults to True.

        Returns:
              GraphListResult: GraphV2 metadata object containing information like
                               graph name, node count, relationship count, etc.
        """

    @abstractmethod
    def filter(
        self,
        G: GraphV2,
        graph_name: str,
        node_filter: str,
        relationship_filter: str,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
    ) -> GraphWithFilterResult:
        """Create a subgraph of a graph based on a filter expression.

        Parameters
        ----------
        G (GraphV2):
            GraphV2 object to filter on
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
            GraphWithFilterResult: Tuple of the filtered graph object and the information like
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
    ) -> GraphWithGenerationStats:
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
            Tuple of the generated graph object and the result object containing stats about the generation.
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

    @property
    @abstractmethod
    def node_properties(self) -> NodePropertiesEndpoints:
        """Endpoints for node label operations."""
        pass

    @property
    @abstractmethod
    def relationships(self) -> RelationshipsEndpoints:
        """Endpoints for relationship operations."""
        pass


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


# cannot use namedtuple + generic result as for python < 3.11 Multiple inheritance with NamedTuple is not supported
class GraphWithFilterResult(NamedTuple):
    graph: GraphV2
    result: GraphFilterResult

    def __enter__(self) -> GraphV2:
        return self.graph

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.graph.drop()


class GraphWithGenerationStats(NamedTuple):
    graph: GraphV2
    result: GraphGenerationStats

    def __enter__(self) -> GraphV2:
        return self.graph

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.graph.drop()
