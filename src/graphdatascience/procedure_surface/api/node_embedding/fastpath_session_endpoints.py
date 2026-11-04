from __future__ import annotations

from abc import abstractmethod
from typing import Any

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.node_embedding.fastpath_endpoints import (
    FastPathEndpoints,
)


class FastPathSessionEndpoints(FastPathEndpoints):
    """FastPath endpoints as exposed by a GDS Session."""

    @abstractmethod
    def compute(
        self,
        G: Graph,
        base_node_label: str,
        event_node_label: str,
        embedding_dimension: int,
        lookback_horizon: int,
        num_time_anchors: int | None = None,
        time_anchor_interval: float | None = None,
        *,
        event_node_categorical_properties: list[str] = [],
        relationship_types: list[str] = ALL_TYPES,
        context_node_label: str | None = None,
        decay_rate: float = 1.0,
        event_node_feature_vector_property: str | None = None,
        first_relationship_type: str | None = None,
        event_node_ignored_category: int = -1,
        next_relationship_type: str | None = None,
        observation_time: float | None = None,
        base_node_observation_time_property: str | None = None,
        random_seed: Any | None = None,
        smoothing_rate: float = 0.0,
        smoothing_window: int = 0,
        event_node_time_property: str | None = None,
        job_id: str | None = None,
    ) -> JobHandle:
        """
        Starts the computation as a job and returns a handle to it.

        The computation runs without materializing its result. Use the returned handle to
        mutate, write or stream the result afterwards.

        Parameters
        ----------
        G
            Graph object to use
        base_node_label
            Node label identifying the nodes to compute embeddings for.
        event_node_label
            Node label marking the event nodes linked to base nodes.
        embedding_dimension
            Output dimensionality of the embeddings
        lookback_horizon
            Maximum age of events (relative to the observation time) that are processed.
        num_time_anchors
            Number of grid points used to discretize elapsed time. Exactly one of `num_time_anchors` and `time_anchor_interval` must be specified.
        time_anchor_interval
            Time interval between anchor embeddings in the grid. Exactly one of `num_time_anchors` and `time_anchor_interval` must be specified.
        event_node_categorical_properties
            Event node properties holding categorical values.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        context_node_label
            Optional node label for context nodes describing events.
        decay_rate
            Controls how quickly the influence of older events decays.
        event_node_feature_vector_property
            Name of the vector property on event nodes holding numerical features.
        first_relationship_type
            Relationship type connecting base nodes to the first event in a chain.
        event_node_ignored_category
            Event category value treated as missing/ignored.
        next_relationship_type
            Relationship type ordering sequential events in a chain.
        observation_time
            Single fixed timestamp the embeddings are produced for.
        base_node_observation_time_property
            Base node property giving a per-node output timestamp.
        random_seed
            Seed for reproducible random vector generation.
        smoothing_rate
            Controls how event embedding similarity decays with time distance.
        smoothing_window
            Window size for aggregating embeddings across nearby grid times.
        event_node_time_property
            Numeric event node property holding the event timestamp.
        job_id
            Identifier for the computation.

        Returns
        -------
        JobHandle
            Handle to the started computation
        """
        pass
