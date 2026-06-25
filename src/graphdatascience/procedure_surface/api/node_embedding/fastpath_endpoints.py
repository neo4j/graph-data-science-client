from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame
from pydantic import Field

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.default_values import ALL_TYPES


class FastPathEndpoints(ABC):
    """
    Endpoints for the FastPath node embedding algorithm.

    FastPath is a lightweight path-embedding algorithm for temporal graphs: it computes
    embeddings for base nodes from their associated event sequences, aggregating event
    vectors by event type and elapsed time. It is only available on the Arrow
    (Aura Graph Analytics) surface.

    See https://neo4j.com/docs/snowflake-graph-analytics/current/algorithms/fastpath/
    """

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        base_node_label: str,
        event_node_label: str,
        mutate_property: str,
        dimension: int,
        max_elapsed_time: int,
        num_elapsed_times: int,
        *,
        categorical_event_properties: list[str] = [],
        relationship_types: list[str] = ALL_TYPES,
        context_node_label: str | None = None,
        decay_factor: float | None = None,
        event_features: str | None = None,
        first_relationship_type: str | None = None,
        ignored_event_category: int | None = None,
        next_relationship_type: str | None = None,
        output_time: float | None = None,
        output_time_property: str | None = None,
        random_seed: Any | None = None,
        smoothing_rate: float | None = None,
        smoothing_window: int | None = None,
        time_node_property: str | None = None,
    ) -> FastPathMutateResult:
        """
        Executes the FastPath algorithm and writes the results back to the graph as a node property.

        Parameters
        ----------
        G
            Graph object to use
        base_node_label
            Node label identifying the nodes to compute embeddings for.
        event_node_label
            Node label marking the event nodes linked to base nodes.
        mutate_property
            Name of the node property to store the results in.
        dimension
            Output dimensionality of the embeddings.
        max_elapsed_time
            Maximum age of events (relative to the output time) that are processed.
        num_elapsed_times
            Number of grid points used to discretize elapsed time.
        categorical_event_properties
            Event node properties holding categorical values.
        relationship_types
            Filter the graph using the given relationship types.
        context_node_label
            Optional node label for context nodes describing events.
        decay_factor
            Controls how quickly the influence of older events decays.
        event_features
            Name of the vector property on event nodes holding numerical features.
        first_relationship_type
            Relationship type connecting base nodes to the first event in a chain.
        ignored_event_category
            Event category value treated as missing/ignored.
        next_relationship_type
            Relationship type ordering sequential events in a chain.
        output_time
            Single fixed timestamp the embeddings are produced for.
        output_time_property
            Base node property giving a per-node output timestamp.
        random_seed
            Seed for reproducible random vector generation.
        smoothing_rate
            Controls how event embedding similarity decays with time distance.
        smoothing_window
            Window size for aggregating embeddings across nearby grid times.
        time_node_property
            Numeric event node property holding the event timestamp.

        Returns
        -------
        FastPathMutateResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def stream(
        self,
        G: Graph,
        base_node_label: str,
        event_node_label: str,
        dimension: int,
        max_elapsed_time: int,
        num_elapsed_times: int,
        *,
        categorical_event_properties: list[str] = [],
        relationship_types: list[str] = ALL_TYPES,
        context_node_label: str | None = None,
        decay_factor: float | None = None,
        event_features: str | None = None,
        first_relationship_type: str | None = None,
        ignored_event_category: int | None = None,
        next_relationship_type: str | None = None,
        output_time: float | None = None,
        output_time_property: str | None = None,
        random_seed: Any | None = None,
        smoothing_rate: float | None = None,
        smoothing_window: int | None = None,
        time_node_property: str | None = None,
    ) -> DataFrame:
        """
        Executes the FastPath algorithm and returns the results as a stream.

        Parameters
        ----------
        G
            Graph object to use
        base_node_label
            Node label identifying the nodes to compute embeddings for.
        event_node_label
            Node label marking the event nodes linked to base nodes.
        dimension
            Output dimensionality of the embeddings.
        max_elapsed_time
            Maximum age of events (relative to the output time) that are processed.
        num_elapsed_times
            Number of grid points used to discretize elapsed time.
        categorical_event_properties
            Event node properties holding categorical values.
        relationship_types
            Filter the graph using the given relationship types.
        context_node_label
            Optional node label for context nodes describing events.
        decay_factor
            Controls how quickly the influence of older events decays.
        event_features
            Name of the vector property on event nodes holding numerical features.
        first_relationship_type
            Relationship type connecting base nodes to the first event in a chain.
        ignored_event_category
            Event category value treated as missing/ignored.
        next_relationship_type
            Relationship type ordering sequential events in a chain.
        output_time
            Single fixed timestamp the embeddings are produced for.
        output_time_property
            Base node property giving a per-node output timestamp.
        random_seed
            Seed for reproducible random vector generation.
        smoothing_rate
            Controls how event embedding similarity decays with time distance.
        smoothing_window
            Window size for aggregating embeddings across nearby grid times.
        time_node_property
            Numeric event node property holding the event timestamp.

        Returns
        -------
        DataFrame
            DataFrame with node IDs and their FastPath embeddings
        """

    @abstractmethod
    def write(
        self,
        G: Graph,
        base_node_label: str,
        event_node_label: str,
        write_property: str,
        dimension: int,
        max_elapsed_time: int,
        num_elapsed_times: int,
        *,
        categorical_event_properties: list[str] = [],
        relationship_types: list[str] = ALL_TYPES,
        context_node_label: str | None = None,
        decay_factor: float | None = None,
        event_features: str | None = None,
        first_relationship_type: str | None = None,
        ignored_event_category: int | None = None,
        next_relationship_type: str | None = None,
        output_time: float | None = None,
        output_time_property: str | None = None,
        random_seed: Any | None = None,
        smoothing_rate: float | None = None,
        smoothing_window: int | None = None,
        time_node_property: str | None = None,
        write_concurrency: int | None = None,
    ) -> FastPathWriteResult:
        """
        Executes the FastPath algorithm and writes the results to Neo4j.

        Parameters
        ----------
        G
            Graph object to use
        base_node_label
            Node label identifying the nodes to compute embeddings for.
        event_node_label
            Node label marking the event nodes linked to base nodes.
        write_property
            Name of the node property to store the results in.
        dimension
            Output dimensionality of the embeddings.
        max_elapsed_time
            Maximum age of events (relative to the output time) that are processed.
        num_elapsed_times
            Number of grid points used to discretize elapsed time.
        categorical_event_properties
            Event node properties holding categorical values.
        relationship_types
            Filter the graph using the given relationship types.
        context_node_label
            Optional node label for context nodes describing events.
        decay_factor
            Controls how quickly the influence of older events decays.
        event_features
            Name of the vector property on event nodes holding numerical features.
        first_relationship_type
            Relationship type connecting base nodes to the first event in a chain.
        ignored_event_category
            Event category value treated as missing/ignored.
        next_relationship_type
            Relationship type ordering sequential events in a chain.
        output_time
            Single fixed timestamp the embeddings are produced for.
        output_time_property
            Base node property giving a per-node output timestamp.
        random_seed
            Seed for reproducible random vector generation.
        smoothing_rate
            Controls how event embedding similarity decays with time distance.
        smoothing_window
            Window size for aggregating embeddings across nearby grid times.
        time_node_property
            Numeric event node property holding the event timestamp.
        write_concurrency
            Number of concurrent threads to use for writing.

        Returns
        -------
        FastPathWriteResult
            Algorithm metrics and statistics
        """


class FastPathMutateResult(BaseResult):
    compute_millis: int = Field(alias="predict_ms")
    mutate_millis: int
    node_properties_written: int
    configuration: dict[str, Any] | None = None


class FastPathWriteResult(BaseResult):
    compute_millis: int = Field(alias="predict_ms")
    write_millis: int
    node_properties_written: int = Field(alias="propertiesWritten")
    configuration: dict[str, Any] | None = None 
