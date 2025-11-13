from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class KMeansEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        node_property: str,
        mutate_property: str,
        *,
        compute_silhouette: bool = False,
        concurrency: int | None = None,
        delta_threshold: float = 0.05,
        initial_sampler: str = "UNIFORM",
        job_id: str | None = None,
        k: int = 10,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        number_of_restarts: int = 1,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        seed_centroids: list[list[float]] | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> KMeansMutateResult:
        """
        Executes the K-Means algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        node_property : str
            The node property to use for clustering
        mutate_property
            Name of the node property to store the results in.
        compute_silhouette : bool | None, default=False
            Whether to compute silhouette coefficient
        concurrency
            Number of concurrent threads to use.
        delta_threshold : float | None, default=0.05
            The convergence threshold for the algorithm
        initial_sampler : str | None, default="UNIFORM"
            The sampling method for initial centroids
        job_id
            Identifier for the computation.
        k : int | None, default=10
            The number of clusters
        log_progress
            Display progress logging.
        max_iterations
            Maximum number of iterations to run.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        number_of_restarts : int | None, default=1
            The number of times the algorithm should be restarted with different initial centers
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        seed_centroids : Optional[list[list[float]]], default=None
            Initial centroids for the algorithm
        sudo
            Disable the memory guard.
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        KMeansMutateResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        node_property: str,
        *,
        compute_silhouette: bool = False,
        concurrency: int | None = None,
        delta_threshold: float = 0.05,
        initial_sampler: str = "UNIFORM",
        job_id: str | None = None,
        k: int = 10,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        number_of_restarts: int = 1,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        seed_centroids: list[list[float]] | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> KMeansStatsResult:
        """
        Executes the K-Means algorithm and returns statistics.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        node_property : str
            The node property to use for clustering
        compute_silhouette : bool | None, default=False
            Whether to compute silhouette coefficient
        concurrency
            Number of concurrent threads to use.
        delta_threshold : float | None, default=0.05
            The convergence threshold for the algorithm
        initial_sampler : str | None, default="UNIFORM"
            The sampling method for initial centroids
        job_id
            Identifier for the computation.
        k : int | None, default=10
            The number of clusters
        log_progress
            Display progress logging.
        max_iterations
            Maximum number of iterations to run.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        number_of_restarts : int | None, default=1
            The number of times the algorithm should be restarted with different initial centers
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        seed_centroids : Optional[list[list[float]]], default=None
            Initial centroids for the algorithm
        sudo
            Disable the memory guard.
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        KMeansStatsResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        node_property: str,
        *,
        compute_silhouette: bool = False,
        concurrency: int | None = None,
        delta_threshold: float = 0.05,
        initial_sampler: str = "UNIFORM",
        job_id: str | None = None,
        k: int = 10,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        number_of_restarts: int = 1,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        seed_centroids: list[list[float]] | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        """
        Executes the K-Means algorithm and returns a stream of results.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        node_property : str
            The node property to use for clustering
        compute_silhouette : bool | None, default=False
            Whether to compute silhouette coefficient
        concurrency
            Number of concurrent threads to use.
        delta_threshold : float | None, default=0.05
            The convergence threshold for the algorithm
        initial_sampler : str | None, default="UNIFORM"
            The sampling method for initial centroids
        job_id
            Identifier for the computation.
        k : int | None, default=10
            The number of clusters
        log_progress
            Display progress logging.
        max_iterations
            Maximum number of iterations to run.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        number_of_restarts : int | None, default=1
            The number of times the algorithm should be restarted with different initial centers
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        seed_centroids : Optional[list[list[float]]], default=None
            Initial centroids for the algorithm
        sudo
            Disable the memory guard.
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        DataFrame
            DataFrame with the algorithm results containing nodeId, communityId, distanceFromCentroid, and silhouette
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        node_property: str,
        write_property: str,
        *,
        compute_silhouette: bool = False,
        concurrency: int | None = None,
        delta_threshold: float = 0.05,
        initial_sampler: str = "UNIFORM",
        job_id: str | None = None,
        k: int = 10,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        number_of_restarts: int = 1,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        seed_centroids: list[list[float]] | None = None,
        sudo: bool = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> KMeansWriteResult:
        """
        Executes the K-Means algorithm and writes the results back to the database.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        node_property : str
            The node property to use for clustering
        write_property
            Name of the node property to store the results in.
        compute_silhouette : bool | None, default=False
            Whether to compute silhouette coefficient
        concurrency
            Number of concurrent threads to use.
        delta_threshold : float | None, default=0.05
            The convergence threshold for the algorithm
        initial_sampler : str | None, default="UNIFORM"
            The sampling method for initial centroids
        job_id
            Identifier for the computation.
        k : int | None, default=10
            The number of clusters
        log_progress
            Display progress logging.
        max_iterations
            Maximum number of iterations to run.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        number_of_restarts : int | None, default=1
            The number of times the algorithm should be restarted with different initial centers
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        seed_centroids : Optional[list[list[float]]], default=None
            Initial centroids for the algorithm
        sudo
            Disable the memory guard.
        username : str | None, default=None
            The username to attribute the procedure run to
        write_concurrency
            Number of concurrent threads to use for writing.Returns
        -------
        KMeansWriteResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        node_property: str,
        *,
        compute_silhouette: bool = False,
        concurrency: int | None = None,
        delta_threshold: float = 0.05,
        initial_sampler: str = "UNIFORM",
        k: int = 10,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        number_of_restarts: int = 1,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        seed_centroids: list[list[float]] | None = None,
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the K-Means algorithm.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph or graph configuration to estimate for
        node_property : str
            The node property to use for clustering
        compute_silhouette : bool | None, default=False
            Whether to compute silhouette coefficient
        concurrency
            Number of concurrent threads to use.
        delta_threshold : float | None, default=0.05
            The convergence threshold for the algorithm
        initial_sampler : str | None, default="UNIFORM"
            The sampling method for initial centroids
        k : int | None, default=10
            The number of clusters
        max_iterations
            Maximum number of iterations to run.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        number_of_restarts : int | None, default=1
            The number of times the algorithm should be restarted with different initial centers
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        seed_centroids : Optional[list[list[float]]], default=None
            Initial centroids for the algorithm

        Returns
        -------
        EstimationResult
            The memory estimation result
        """
        pass


class KMeansMutateResult(BaseResult):
    average_distance_to_centroid: float
    average_silhouette: float
    centroids: list[list[float]]
    community_distribution: dict[str, int | float]
    compute_millis: int
    configuration: dict[str, Any]
    mutate_millis: int
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int


class KMeansStatsResult(BaseResult):
    average_distance_to_centroid: float
    average_silhouette: float
    centroids: list[list[float]]
    community_distribution: dict[str, int | float]
    compute_millis: int
    configuration: dict[str, Any]
    post_processing_millis: int
    pre_processing_millis: int


class KMeansWriteResult(BaseResult):
    average_distance_to_centroid: float
    average_silhouette: float
    centroids: list[list[float]]
    community_distribution: dict[str, int | float]
    compute_millis: int
    configuration: dict[str, Any]
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int
    write_millis: int
