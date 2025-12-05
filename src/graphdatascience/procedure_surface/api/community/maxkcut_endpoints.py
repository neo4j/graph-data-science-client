from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class MaxKCutEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        concurrency: int | None = None,
        iterations: int = 8,
        job_id: str | None = None,
        k: int = 2,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        sudo: bool = False,
        username: str | None = None,
        vns_max_neighborhood_order: int = 0,
    ) -> MaxKCutMutateResult:
        """
        Executes the Approximate Maximum k-cut algorithm and writes the results to the in-memory graph as node properties.

        The Approximate Maximum k-cut algorithm is a community detection algorithm that partitions a graph into k communities
        such that the sum of weights of edges between different communities is maximized. It uses a
        variable neighborhood search (VNS) approach to find high-quality cuts.

        Parameters
        ----------
        G
           Graph object to use
        mutate_property
            Name of the node property to store the results in.
        concurrency
            Number of concurrent threads to use.
        iterations
            Number of iterations to run.
        job_id
            Identifier for the computation.
        k
            Number of communities to detect.
        log_progress
            Display progress logging.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        vns_max_neighborhood_order : int | None, default=0
            The maximum neighborhood order for the Variable Neighborhood Search. Higher values may
            lead to better results but increase computation time.

        Returns
        -------
        MaxKCutMutateResult
            Algorithm metrics and statistics including the cut cost and processing times
        """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        iterations: int = 8,
        job_id: str | None = None,
        k: int = 2,
        log_progress: bool = True,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        sudo: bool = False,
        username: str | None = None,
        vns_max_neighborhood_order: int = 0,
    ) -> DataFrame:
        """
        Executes the Approximate Maximum k-cut algorithm and returns a stream of results.

        The Approximate Maximum k-cut algorithm partitions a graph into k communities to maximize the cut cost.
        This method returns the community assignment for each node as a stream.

        Parameters
        ----------
        G
           Graph object to use
        concurrency
            Number of concurrent threads to use.
        iterations
            Number of iterations to run.
        job_id
            Identifier for the computation.
        k
            Number of communities to detect.
        log_progress
            Display progress logging.
        min_community_size
            Minimum size for communities to be included in results.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        vns_max_neighborhood_order : int | None, default=0
            The maximum neighborhood order for the Variable Neighborhood Search. Higher values may
            lead to better results but increase computation time.

        Returns
        -------
        DataFrame
            A DataFrame with columns:
            - nodeId: The node identifier
            - communityId: The community assignment for the node
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        concurrency: int | None = None,
        iterations: int = 8,
        k: int = 2,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
        vns_max_neighborhood_order: int = 0,
    ) -> EstimationResult:
        """
        Estimate the memory requirements for running the Approximate Maximum k-cut algorithm.

        This method provides memory estimates without actually running the algorithm, helping you
        determine if you have sufficient memory available.

        Parameters
        ----------
        G
           Graph object to use or a dictionary representing the graph dimensions.
        concurrency
            Number of concurrent threads to use.
        iterations
            Number of iterations to run.
        k
            Number of communities to detect.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        vns_max_neighborhood_order : int | None, default=0
            The maximum neighborhood order for the Variable Neighborhood Search

        Returns
        -------
        EstimationResult
            The memory estimation result including required memory in bytes and as heap percentage
        """


class MaxKCutMutateResult(BaseResult):
    cut_cost: float
    compute_millis: int
    configuration: dict[str, Any]
    mutate_millis: int
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int
