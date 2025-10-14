from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.similarity.knn_endpoints import (
    KnnMutateResult,
    KnnStatsResult,
    KnnWriteResult,
)


class KnnFilteredEndpoints(ABC):
    """Base class for Filtered K-Nearest Neighbors endpoints."""

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        mutate_property: str,
        node_properties: str | list[str] | dict[str, str],
        source_node_filter: str,
        target_node_filter: str,
        seed_target_nodes: bool | None = None,
        top_k: int | None = None,
        similarity_cutoff: float | None = None,
        delta_threshold: float | None = None,
        max_iterations: int | None = None,
        sample_rate: float | None = None,
        perturbation_rate: float | None = None,
        random_joins: int | None = None,
        random_seed: int | None = None,
        initial_sampler: Any | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> KnnMutateResult:
        """Run filtered K-Nearest Neighbors in mutate mode."""
        ...

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        node_properties: str | list[str] | dict[str, str],
        source_node_filter: str,
        target_node_filter: str,
        seed_target_nodes: bool | None = None,
        top_k: int | None = None,
        similarity_cutoff: float | None = None,
        delta_threshold: float | None = None,
        max_iterations: int | None = None,
        sample_rate: float | None = None,
        perturbation_rate: float | None = None,
        random_joins: int | None = None,
        random_seed: int | None = None,
        initial_sampler: Any | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> KnnStatsResult:
        """Run filtered K-Nearest Neighbors in stats mode."""
        ...

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        node_properties: str | list[str] | dict[str, str],
        source_node_filter: str,
        target_node_filter: str,
        seed_target_nodes: bool | None = None,
        top_k: int | None = None,
        similarity_cutoff: float | None = None,
        delta_threshold: float | None = None,
        max_iterations: int | None = None,
        sample_rate: float | None = None,
        perturbation_rate: float | None = None,
        random_joins: int | None = None,
        random_seed: int | None = None,
        initial_sampler: Any | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> DataFrame:
        """Run filtered K-Nearest Neighbors in stream mode."""
        ...

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        write_property: str,
        node_properties: str | list[str] | dict[str, str],
        source_node_filter: str,
        target_node_filter: str,
        seed_target_nodes: bool | None = None,
        top_k: int | None = None,
        similarity_cutoff: float | None = None,
        delta_threshold: float | None = None,
        max_iterations: int | None = None,
        sample_rate: float | None = None,
        perturbation_rate: float | None = None,
        random_joins: int | None = None,
        random_seed: int | None = None,
        initial_sampler: Any | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        write_concurrency: int | None = None,
        write_to_result_store: bool | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> KnnWriteResult:
        """Run filtered K-Nearest Neighbors in write mode."""
        ...

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        node_properties: str | list[str] | dict[str, str],
        source_node_filter: str,
        target_node_filter: str,
        seed_target_nodes: bool | None = None,
        top_k: int | None = None,
        similarity_cutoff: float | None = None,
        delta_threshold: float | None = None,
        max_iterations: int | None = None,
        sample_rate: float | None = None,
        perturbation_rate: float | None = None,
        random_joins: int | None = None,
        random_seed: int | None = None,
        initial_sampler: Any | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        username: str | None = None,
        concurrency: Any | None = None,
    ) -> EstimationResult:
        """Estimate filtered K-Nearest Neighbors execution requirements.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on.
        node_properties : str | list[str]
            The node properties to use for similarity computation.
        mutate_property : str
            The relationship property to store the similarity score in.
        mutate_relationship_type : str
            The relationship type to use for the new relationships.
        source_node_filter : str | None, default=None
            A Cypher expression to filter which nodes can be sources in the similarity computation.
        target_node_filter : str | None, default=None
            A Cypher expression to filter which nodes can be targets in the similarity computation.
        seed_target_nodes : bool | None, default=None
            Whether to use a seeded approach for target node selection.
        similarity_cutoff : float | None, default=None
            The threshold for similarity scores.
        perturbation_rate : float | None, default=None
            The rate at which to perturb the similarity graph.
        delta_threshold : float | None, default=None
            The threshold for convergence assessment.
        sample_rate : float | None, default=None
            The sampling rate for the algorithm.
        random_joins : int | None, default=None
            The number of random joins to perform.
        initial_sampler : str | None, default=None
            The initial sampling strategy.
        max_iterations : int | None, default=None
            The maximum number of iterations to run.
        top_k : int | None, default=None
            The number of nearest neighbors to find for each node.
        random_seed : int | None, default=None
            The seed for the random number generator.
        concurrency : int | None, default=None
            Concurrency configuration.
        job_id : str | None, default=None
            Job ID for the operation.
        log_progress : bool | None, default=None
            Whether to log progress.
        sudo : bool | None, default=None
            Run the algorithm with elevated privileges.
        username : str | None, default=None
            Username for the operation.
        **kwargs : Any
            Additional parameters.

        Returns
        -------
        KnnMutateResult
            Object containing metadata from the execution.
        """
        ...
