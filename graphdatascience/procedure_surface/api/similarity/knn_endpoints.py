from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.similarity.knn_filtered_endpoints import KnnFilteredEndpoints
from graphdatascience.procedure_surface.api.similarity.knn_results import (
    KnnMutateResult,
    KnnStatsResult,
    KnnWriteResult,
)


class KnnEndpoints(ABC):
    @property
    @abstractmethod
    def filtered(self) -> KnnFilteredEndpoints:
        pass

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        mutate_property: str,
        node_properties: str | list[str] | dict[str, str],
        top_k: int = 10,
        similarity_cutoff: float = 0.0,
        delta_threshold: float = 0.001,
        max_iterations: int = 100,
        sample_rate: float = 0.5,
        perturbation_rate: float = 0.0,
        random_joins: int = 10,
        random_seed: int | None = None,
        initial_sampler: str = "UNIFORM",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> KnnMutateResult:
        """
        Runs the K-Nearest Neighbors algorithm and stores the results as new relationships in the graph catalog.

        Parameters
        ----------
        G
           Graph object to use
        mutate_relationship_type
            Name of the relationship type to store the results in.
        mutate_property
            Name of the node property to store the results in.
        node_properties : str | list[str] | dict[str, str],
            Node properties to use for the similarity computation.
        top_k
            Number of most similar nodes to return for each node.
        similarity_cutoff : float, default=0.0
            The threshold for similarity scores.
        delta_threshold
            Minimum change between iterations.
        max_iterations
            Maximum number of iterations to run.
        sample_rate : float, default=0.5
            The sampling rate for the algorithm.
        perturbation_rate : float, default=0.0
            The rate at which to perturb the similarity graph.
        random_joins : int, default=10
            The number of random joins to perform.
        random_seed
            Seed for random number generation to ensure reproducible results.
        initial_sampler
            Sampling strategy for the initial nearest neighbors.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

        Returns
        -------
        KnnMutateResult
            Object containing metadata from the execution.
        """

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        node_properties: str | list[str] | dict[str, str],
        top_k: int = 10,
        similarity_cutoff: float = 0.0,
        delta_threshold: float = 0.001,
        max_iterations: int = 100,
        sample_rate: float = 0.5,
        perturbation_rate: float = 0.0,
        random_joins: int = 10,
        random_seed: int | None = None,
        initial_sampler: str = "UNIFORM",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> KnnStatsResult:
        """
        Runs the K-Nearest Neighbors algorithm and returns execution statistics.

        Parameters
        ----------
        G
           Graph object to use
        node_properties:  str | list[str] | dict[str, str],
            Node properties to use for the similarity computation.
        top_k
            Number of most similar nodes to return for each node.
        similarity_cutoff : float, default=0.0
            The threshold for similarity scores.
        delta_threshold
            Minimum change between iterations.
        max_iterations
            Maximum number of iterations to run.
        sample_rate : float, default=0.5
            The sampling rate for the algorithm.
        perturbation_rate : float, default=0.0
            The rate at which to perturb the similarity graph.
        random_joins : int, default=10
            The number of random joins to perform.
        random_seed
            Seed for random number generation to ensure reproducible results.
        initial_sampler
            Sampling strategy for the initial nearest neighbors.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

        Returns
        -------
        KnnStatsResult
            Object containing execution statistics and algorithm-specific results.
        """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        node_properties: str | list[str] | dict[str, str],
        top_k: int = 10,
        similarity_cutoff: float = 0.0,
        delta_threshold: float = 0.001,
        max_iterations: int = 100,
        sample_rate: float = 0.5,
        perturbation_rate: float = 0.0,
        random_joins: int = 10,
        random_seed: int | None = None,
        initial_sampler: str = "UNIFORM",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Runs the K-Nearest Neighbors algorithm and returns the result as a DataFrame.

        Parameters
        ----------
        G
           Graph object to use
        node_properties:  str | list[str] | dict[str, str],
            Node properties to use for the similarity computation.
        top_k
            Number of most similar nodes to return for each node.
        similarity_cutoff : float, default=0.0
            The threshold for similarity scores.
        delta_threshold
            Minimum change between iterations.
        max_iterations
            Maximum number of iterations to run.
        sample_rate : float, default=0.5
            The sampling rate for the algorithm.
        perturbation_rate : float, default=0.0
            The rate at which to perturb the similarity graph.
        random_joins : int, default=10
            The number of random joins to perform.
        random_seed
            Seed for random number generation to ensure reproducible results.
        initial_sampler
            Sampling strategy for the initial nearest neighbors.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

        Returns
        -------
        DataFrame
            The similarity results as a DataFrame with columns 'node1', 'node2', and 'similarity'.
        """

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        write_property: str,
        node_properties: str | list[str] | dict[str, str],
        top_k: int = 10,
        similarity_cutoff: float = 0.0,
        delta_threshold: float = 0.001,
        max_iterations: int = 100,
        sample_rate: float = 0.5,
        perturbation_rate: float = 0.0,
        random_joins: int = 10,
        random_seed: int | None = None,
        initial_sampler: str = "UNIFORM",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> KnnWriteResult:
        """
        Runs the K-Nearest Neighbors algorithm and writes the results back to the database.

        Parameters
        ----------
        G
           Graph object to use
        write_relationship_type : str
            The relationship type to use for the new relationships.
        write_property
            Name of the node property to store the results in.
        node_properties:  str | list[str] | dict[str, str],
            Node properties to use for the similarity computation.
        top_k
            Number of most similar nodes to return for each node.
        similarity_cutoff : float, default=0.0
            The threshold for similarity scores.
        delta_threshold
            Minimum change between iterations.
        max_iterations
            Maximum number of iterations to run.
        sample_rate : float, default=0.5
            The sampling rate for the algorithm.
        perturbation_rate : float, default=0.0
            The rate at which to perturb the similarity graph.
        random_joins : int, default=10
            The number of random joins to perform.
        random_seed
            Seed for random number generation to ensure reproducible results.
        initial_sampler
            Sampling strategy for the initial nearest neighbors.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        write_concurrency
            Number of concurrent threads to use for writing.Returns
        -------
        KnnWriteResult
            Object containing metadata from the execution.
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        node_properties: str | list[str] | dict[str, str],
        top_k: int = 10,
        similarity_cutoff: float = 0.0,
        delta_threshold: float = 0.001,
        max_iterations: int = 100,
        sample_rate: float = 0.5,
        perturbation_rate: float = 0.0,
        random_joins: int = 10,
        random_seed: int | None = None,
        initial_sampler: str = "UNIFORM",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the K-Nearest Neighbors algorithm.

        Parameters
        ----------
        G
           Graph object to use or a dictionary representing the graph dimensions.
        node_properties:  str | list[str] | dict[str, str],
            Node properties to use for the similarity computation.
        top_k
            Number of most similar nodes to return for each node.
        similarity_cutoff : float, default=0.0
            The threshold for similarity scores.
        delta_threshold
            Minimum change between iterations.
        max_iterations
            Maximum number of iterations to run.
        sample_rate : float, default=0.5
            The sampling rate for the algorithm.
        perturbation_rate : float, default=0.0
            The rate at which to perturb the similarity graph.
        random_joins : int, default=10
            The number of random joins to perform.
        random_seed
            Seed for random number generation to ensure reproducible results.
        initial_sampler
            Sampling strategy for the initial nearest neighbors.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

        Returns
        -------
        EstimationResult
            Object containing the estimated memory requirements.
        """
