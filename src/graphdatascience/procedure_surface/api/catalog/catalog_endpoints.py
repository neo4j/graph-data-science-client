from __future__ import annotations

from abc import ABC, abstractmethod
from types import TracebackType
from typing import Any, NamedTuple, Type

from pandas import DataFrame
from pydantic import field_validator

from graphdatascience.graph.graph_api import Graph
from graphdatascience.graph.graph_info import GraphInfoWithDegrees
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.dataset_endpoints import DatasetEndpoints
from graphdatascience.procedure_surface.api.catalog.graph_export_endpoints import GraphExportEndpoints
from graphdatascience.procedure_surface.api.catalog.graph_sampling_endpoints import GraphSamplingEndpoints
from graphdatascience.procedure_surface.api.catalog.node_label_endpoints import NodeLabelEndpoints
from graphdatascience.procedure_surface.api.catalog.node_properties_endpoints import NodePropertiesEndpoints
from graphdatascience.procedure_surface.api.catalog.node_property_endpoints import NodePropertyEndpoints
from graphdatascience.procedure_surface.api.catalog.relationship_properties_endpoints import (
    RelationshipPropertiesEndpoints,
)
from graphdatascience.procedure_surface.api.catalog.relationship_property_endpoints import (
    RelationshipPropertyEndpoints,
)
from graphdatascience.procedure_surface.api.catalog.relationships_endpoints import RelationshipsEndpoints


def validate_distinct_from_source(graph_name: str, source_graph: Graph) -> None:
    """Raise ``ValueError`` if the target graph name equals the source graph's name.

    Creating a derived graph (e.g. a filter or sample) with the same name as the graph it
    is derived from would overwrite the source mid-operation.
    """
    source_name = source_graph.name()
    if graph_name == source_name:
        raise ValueError(
            f"The target graph name '{graph_name}' must not equal the source graph name '{source_name}', "
            "as this would overwrite the graph being read from."
        )


class CatalogEndpoints(ABC):
    @abstractmethod
    def get(self, graph_name: str) -> Graph:
        """Retrieve a handle to a graph from the graph catalog.

        Parameters
        ----------
        graph_name
            Name of the graph

        Returns
        -------
        Graph
            A handle to the graph.
        """
        pass

    @abstractmethod
    def exists(self, graph_name: str) -> bool:
        """Check if a graph exists in the catalog.

        Parameters
        ----------
        graph_name
            Name of the graph

        Returns
        -------
        bool
            True if the graph exists, False otherwise.
        """
        pass

    @abstractmethod
    def construct(
        self,
        graph_name: str,
        nodes: DataFrame | list[DataFrame],
        relationships: DataFrame | list[DataFrame] | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        batch_size: int = 100000,
        overwrite: bool = False,
    ) -> Graph:
        """Construct a graph from a list of node and relationship dataframes.

        Parameters
        ----------
        graph_name
            Name of the graph to be created
        nodes
            Node dataframes. A dataframe should follow the schema:

            - `nodeId` to identify uniquely the node overall dataframes
            - `labels` to specify the labels of the node as a list of strings (optional)
            - other columns are treated as node properties
        relationships
            Relationship dataframes. A dataframe should follow the schema:

            - `sourceNodeId` to identify the start node of the relationship
            - `targetNodeId` to identify the end node of the relationship
            - `relationshipType` to specify the type of the relationship (optional)
            - other columns are treated as relationship properties
        concurrency
            Number of concurrent threads to use.
        undirected_relationship_types
            List of relationship types to treat as undirected.
        inverse_indexed_relationship_types
            List of relationship types for which to create an inverse index.
        batch_size
            Batch size to use when sending data to GDS.
        overwrite
            If `True`, drop an existing graph with the same name before constructing the new one.
            Defaults to `False`.

        Returns
        -------
        Graph
            Constructed graph object.
        """

    @property
    def datasets(self) -> DatasetEndpoints:
        """
        Endpoints for loading predefined datasets into the graph catalog.
        """
        return DatasetEndpoints(self.construct)

    @abstractmethod
    def list(self, G: Graph | str | None = None) -> list[GraphInfoWithDegrees]:
        """List graphs in the graph catalog.

        Parameters
        ----------
        G
            GraphV2 object or name to filter results. If None, list all graphs.

        Returns
        -------
        list[GraphInfoWithDegrees]
            List of graph metadata objects containing information like node count.
        """
        pass

    @abstractmethod
    def filter(
        self,
        G: Graph,
        graph_name: str,
        node_filter: str,
        relationship_filter: str,
        parameters: dict[str, Any] | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        overwrite: bool = False,
    ) -> GraphWithFilterResult:
        """Create a subgraph of a graph based on a filter expression.

        Parameters
        ----------
        G
           Graph object to use
        graph_name
            Name of the graph to be created
        node_filter
            Filter expression for nodes
        relationship_filter
            Filter expression for relationships
        parameters
            A map of user-defined query parameters that are passed into the node and relationship filters.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        overwrite
            If `True`, drop an existing graph with the same name before creating the filtered subgraph.
            Defaults to `False`.

        Returns
        -------
        GraphWithFilterResult
            tuple of the filtered graph object and the information like graph name, node count, relationship count, etc.
        """
        pass

    @abstractmethod
    def generate(
        self,
        graph_name: str,
        node_count: int,
        average_degree: float,
        *,
        relationship_distribution: str | None = "UNIFORM",
        relationship_seed: int | None = None,
        relationship_property: RelationshipPropertySpec | None = None,
        orientation: str | None = "NATURAL",
        aggregation: str | None = "NONE",
        allow_self_loops: bool | None = False,
        read_concurrency: int | None = None,
        job_id: str | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        overwrite: bool = False,
    ) -> GraphWithGenerationStats:
        """
        Generates a random graph and store it in the graph catalog.

        Parameters
        ----------
        graph_name
            Name of the graph to be created
        node_count
            The number of nodes in the generated graph
        average_degree
            The average out-degree of the generated nodes
        relationship_distribution
            Determines the relationship distribution strategy.
        relationship_seed
            Seed value for generating deterministic relationships.
        relationship_property
            Configure generated relationship properties.
        orientation
            Specifies the orientation of the generated relationships.
        aggregation
            The relationship aggregation method of Relationship Projection.
        allow_self_loops
            Whether nodes in the graph can have relationships where start and end nodes are the same.
        read_concurrency
            Number of concurrent threads/processes to use during graph generation.
        job_id
            Identifier for the computation.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        overwrite
            If `True`, drop an existing graph with the same name before generating the new one.
            Defaults to `False`.

        Returns
        -------
        GraphWithGenerationStats
            tuple of the generated graph object and the result object containing stats about the generation.
        """

    @property
    @abstractmethod
    def export(self) -> GraphExportEndpoints:
        """Endpoints for exporting graphs to a new database or CSV files."""
        pass

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
        """Endpoints for node property operations."""
        pass

    @property
    def node_property(self) -> NodePropertyEndpoints:
        """Endpoints for streaming a single node property."""
        return NodePropertyEndpoints(self.node_properties)

    @property
    @abstractmethod
    def relationships(self) -> RelationshipsEndpoints:
        """Endpoints for relationship operations."""
        pass

    @property
    def relationship_property(self) -> RelationshipPropertyEndpoints:
        """Endpoints for streaming a single relationship property."""
        return RelationshipPropertyEndpoints(self.relationships)

    @property
    def relationship_properties(self) -> RelationshipPropertiesEndpoints:
        """Endpoints for streaming several relationship properties."""
        return RelationshipPropertiesEndpoints(self.relationships)


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
    relationship_seed: int | None
    average_degree: float
    relationship_distribution: str
    relationship_property: RelationshipPropertySpec | None

    @field_validator("relationship_property", mode="before")
    @classmethod
    def check_empty_property(cls, value: Any) -> Any:
        return value or None


class RelationshipPropertySpec(BaseResult):
    name: str
    type: str
    min: float | None = None
    max: float | None = None
    value: float | None = None

    @staticmethod
    def fixed(name: str, value: float) -> RelationshipPropertySpec:
        return RelationshipPropertySpec(name=name, type="FIXED", value=value)

    @staticmethod
    def random(name: str, min: float, max: float) -> RelationshipPropertySpec:
        return RelationshipPropertySpec(name=name, type="RANDOM", min=min, max=max)


# cannot use namedtuple + generic result as for python < 3.11 Multiple inheritance with NamedTuple is not supported
class GraphWithFilterResult(NamedTuple):
    graph: Graph
    result: GraphFilterResult

    def __enter__(self) -> Graph:
        return self.graph

    def __exit__(
        self,
        exception_type: Type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.graph.drop()


class GraphWithGenerationStats(NamedTuple):
    graph: Graph
    result: GraphGenerationStats

    def __enter__(self) -> Graph:
        return self.graph

    def __exit__(
        self,
        exception_type: Type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.graph.drop()
