from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class ModularityEndpoints(ABC):
    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        community_property: str,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> ModularityStatsResult:
        """
        Executes the Modularity algorithm and returns statistics.

        Parameters
        ----------
        G
            Graph object to use
        community_property
            Name of the node property containing community assignments.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.

        Returns
        -------
        ModularityStatsResult
            Algorithm statistics including communityCount, modularity, nodeCount, and relationshipCount
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        community_property: str,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        """
        Executes the Modularity algorithm and returns a stream of results.

        Parameters
        ----------
        G
            Graph object to use
        community_property
            Name of the node property containing community assignments.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.

        Returns
        -------
        DataFrame
            DataFrame with the algorithm results containing 'communityId' and 'modularity' columns
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        community_property: str,
        *,
        concurrency: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of the modularity algorithm.

        Parameters
        ----------
        G
            Graph object to use or a dictionary representing the graph dimensions.
        community_property
            Name of the node property containing community assignments.
        concurrency
            Number of concurrent threads to use.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.

        Returns
        -------
        EstimationResult
            An object containing the result of the estimation
        """
        pass


class ModularityStatsResult(BaseResult):
    """
    Result object for the modularity stats algorithm.
    """

    community_count: int
    modularity: float
    node_count: int
    relationship_count: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]
