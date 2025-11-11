from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.similarity.knn_results import (
    KnnMutateResult,
    KnnStatsResult,
    KnnWriteResult,
)


class KnnFilteredEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        mutate_property: str,
        node_properties: str | list[str] | dict[str, str],
        source_node_filter: str,
        target_node_filter: str,
        seed_target_nodes: bool = False,
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
        Runs the Filtered K-Nearest Neighbors algorithm and stores the results as new relationships in the graph catalog.

        The Filtered K-Nearest Neighbors algorithm computes a distance value for node pairs in the graph with customizable source and target node filters, creating new relationships between each node and its k nearest neighbors within the filtered subset.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on.
        mutate_relationship_type : str
            The relationship type to use for the new relationships.
        mutate_property : str
            The relationship property to store the similarity score in.
        node_properties : str | list[str] | dict[str, str]
            The node properties to use for similarity computation.
        source_node_filter : str
            A Cypher expression to filter which nodes can be sources in the similarity computation.
        target_node_filter : str
            A Cypher expression to filter which nodes can be targets in the similarity computation.
        seed_target_nodes : bool | None, default=None
            Whether to use a seeded approach for target node selection.
        top_k : int, default=10
            The number of nearest neighbors to find for each node.
        similarity_cutoff : float, default=0.0
            The threshold for similarity scores.
        delta_threshold : float, default=0.001
            The threshold for convergence assessment.
        max_iterations : int, default=100
            The maximum number of iterations to run.
        sample_rate : float, default=0.5
            The sampling rate for the algorithm.
        perturbation_rate : float, default=0.0
            The rate at which to perturb the similarity graph.
        random_joins : int, default=10
            The number of random joins to perform.
        random_seed : int | None, default=None
            The seed for the random number generator.
        initial_sampler : str, default="UNIFORM"
            The initial sampling strategy.
        relationship_types : list[str]
            Filter on relationship types.
        node_labels : list[str]
            Filter on node labels.
        sudo : bool, default=False
            Run the algorithm with elevated privileges.
        log_progress : bool, default=True
            Whether to log progress.
        username : str | None, default=None
            Username for the operation.
        concurrency : int | None, default=None
            Concurrency configuration.
        job_id : str | None, default=None
            Job ID for the operation.

        Returns
        -------
        KnnMutateResult
            Object containing metadata from the execution.
        """
        ...

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        node_properties: str | list[str] | dict[str, str],
        source_node_filter: str,
        target_node_filter: str,
        seed_target_nodes: bool = False,
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
        Runs the Filtered K-Nearest Neighbors algorithm and returns execution statistics.

        The Filtered K-Nearest Neighbors algorithm computes a distance value for node pairs in the graph with customizable source and target node filters, creating new relationships between each node and its k nearest neighbors within the filtered subset.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on.
        node_properties : str | list[str] | dict[str, str]
            The node properties to use for similarity computation.
        source_node_filter : str
            A Cypher expression to filter which nodes can be sources in the similarity computation.
        target_node_filter : str
            A Cypher expression to filter which nodes can be targets in the similarity computation.
        seed_target_nodes : bool | None, default=None
            Whether to use a seeded approach for target node selection.
        top_k : int, default=10
            The number of nearest neighbors to find for each node.
        similarity_cutoff : float, default=0.0
            The threshold for similarity scores.
        delta_threshold : float, default=0.001
            The threshold for convergence assessment.
        max_iterations : int, default=100
            The maximum number of iterations to run.
        sample_rate : float, default=0.5
            The sampling rate for the algorithm.
        perturbation_rate : float, default=0.0
            The rate at which to perturb the similarity graph.
        random_joins : int, default=10
            The number of random joins to perform.
        random_seed : int | None, default=None
            The seed for the random number generator.
        initial_sampler : str, default="UNIFORM"
            The initial sampling strategy.
        relationship_types : list[str]
            Filter on relationship types.
        node_labels : list[str]
            Filter on node labels.
        sudo : bool, default=False
            Run the algorithm with elevated privileges.
        log_progress : bool, default=True
            Whether to log progress.
        username : str | None, default=None
            Username for the operation.
        concurrency : int | None, default=None
            Concurrency configuration.
        job_id : str | None, default=None
            Job ID for the operation.

        Returns
        -------
        KnnStatsResult
            Object containing execution statistics and algorithm-specific results.
        """
        ...

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        node_properties: str | list[str] | dict[str, str],
        source_node_filter: str,
        target_node_filter: str,
        seed_target_nodes: bool = False,
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
        Runs the Filtered K-Nearest Neighbors algorithm and returns the result as a DataFrame.

        The Filtered K-Nearest Neighbors algorithm computes a distance value for node pairs in the graph with customizable source and target node filters, creating new relationships between each node and its k nearest neighbors within the filtered subset.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on.
        node_properties : str | list[str] | dict[str, str]
            The node properties to use for similarity computation.
        source_node_filter : str
            A Cypher expression to filter which nodes can be sources in the similarity computation.
        target_node_filter : str
            A Cypher expression to filter which nodes can be targets in the similarity computation.
        seed_target_nodes : bool | None, default=None
            Whether to use a seeded approach for target node selection.
        top_k : int, default=10
            The number of nearest neighbors to find for each node.
        similarity_cutoff : float, default=0.0
            The threshold for similarity scores.
        delta_threshold : float, default=0.001
            The threshold for convergence assessment.
        max_iterations : int, default=100
            The maximum number of iterations to run.
        sample_rate : float, default=0.5
            The sampling rate for the algorithm.
        perturbation_rate : float, default=0.0
            The rate at which to perturb the similarity graph.
        random_joins : int, default=10
            The number of random joins to perform.
        random_seed : int | None, default=None
            The seed for the random number generator.
        initial_sampler : str, default="UNIFORM"
            The initial sampling strategy.
        relationship_types : list[str]
            Filter on relationship types.
        node_labels : list[str]
            Filter on node labels.
        sudo : bool, default=False
            Run the algorithm with elevated privileges.
        log_progress : bool, default=True
            Whether to log progress.
        username : str | None, default=None
            Username for the operation.
        concurrency : int | None, default=None
            Concurrency configuration.
        job_id : str | None, default=None
            Job ID for the operation.

        Returns
        -------
        DataFrame
            The similarity results as a DataFrame with columns 'node1', 'node2', and 'similarity'.
        """
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
        seed_target_nodes: bool = False,
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
        write_concurrency: int | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> KnnWriteResult:
        """
        Runs the Filtered K-Nearest Neighbors algorithm and writes the results back to the database.

        The Filtered K-Nearest Neighbors algorithm computes a distance value for node pairs in the graph with customizable source and target node filters, creating new relationships between each node and its k nearest neighbors within the filtered subset.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on.
        write_relationship_type : str
            The relationship type to use for the new relationships.
        write_property : str
            The relationship property to store the similarity score in.
        node_properties : str | list[str] | dict[str, str]
            The node properties to use for similarity computation.
        source_node_filter : str
            A Cypher expression to filter which nodes can be sources in the similarity computation.
        target_node_filter : str
            A Cypher expression to filter which nodes can be targets in the similarity computation.
        seed_target_nodes : bool | None, default=None
            Whether to use a seeded approach for target node selection.
        top_k : int, default=10
            The number of nearest neighbors to find for each node.
        similarity_cutoff : float, default=0.0
            The threshold for similarity scores.
        delta_threshold : float, default=0.001
            The threshold for convergence assessment.
        max_iterations : int, default=100
            The maximum number of iterations to run.
        sample_rate : float, default=0.5
            The sampling rate for the algorithm.
        perturbation_rate : float, default=0.0
            The rate at which to perturb the similarity graph.
        random_joins : int, default=10
            The number of random joins to perform.
        random_seed : int | None, default=None
            The seed for the random number generator.
        initial_sampler : str, default="UNIFORM"
            The initial sampling strategy.
        relationship_types : list[str]
            Filter on relationship types.
        node_labels : list[str]
            Filter on node labels.
        write_concurrency : int | None, default=None
            Concurrency for writing results.
        sudo : bool, default=False
            Run the algorithm with elevated privileges.
        log_progress : bool, default=True
            Whether to log progress.
        username : str | None, default=None
            Username for the operation.
        concurrency : int | None, default=None
            Concurrency configuration.
        job_id : str | None, default=None
            Job ID for the operation.

        Returns
        -------
        KnnWriteResult
            Object containing metadata from the execution.
        """
        ...

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        node_properties: str | list[str] | dict[str, str],
        source_node_filter: str,
        target_node_filter: str,
        seed_target_nodes: bool = False,
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
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the Filtered K-Nearest Neighbors algorithm.

        The Filtered K-Nearest Neighbors algorithm computes a distance value for node pairs in the graph with customizable source and target node filters, creating new relationships between each node and its k nearest neighbors within the filtered subset.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on.
        node_properties : str | list[str] | dict[str, str]
            The node properties to use for similarity computation.
        source_node_filter : str
            A Cypher expression to filter which nodes can be sources in the similarity computation.
        target_node_filter : str
            A Cypher expression to filter which nodes can be targets in the similarity computation.
        seed_target_nodes : bool | None, default=None
            Whether to use a seeded approach for target node selection.
        top_k : int, default=10
            The number of nearest neighbors to find for each node.
        similarity_cutoff : float, default=0.0
            The threshold for similarity scores.
        delta_threshold : float, default=0.001
            The threshold for convergence assessment.
        max_iterations : int, default=100
            The maximum number of iterations to run.
        sample_rate : float, default=0.5
            The sampling rate for the algorithm.
        perturbation_rate : float, default=0.0
            The rate at which to perturb the similarity graph.
        random_joins : int, default=10
            The number of random joins to perform.
        random_seed : int | None, default=None
            The seed for the random number generator.
        initial_sampler : str, default="UNIFORM"
            The initial sampling strategy.
        relationship_types : list[str]
            Filter on relationship types.
        node_labels : list[str]
            Filter on node labels.
        sudo : bool, default=False
            Run the algorithm with elevated privileges.
        username : str | None, default=None
            Username for the operation.
        concurrency : int | None, default=None
            Concurrency configuration.

        Returns
        -------
        EstimationResult
            Object containing the estimated memory requirements.
        """
        ...
