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
        G : GraphV2
            The graph to run the algorithm on.
        node_property : str
            The node property to use for clustering
        mutate_property : str
            The property name to store the community ID for each node
        compute_silhouette : bool | None, default=False
            Whether to compute silhouette coefficient
        concurrency : int | None, default=None
            The number of concurrent threads
        delta_threshold : float | None, default=0.05
            The convergence threshold for the algorithm
        initial_sampler : str | None, default="UNIFORM"
            The sampling method for initial centroids
        job_id : str | None, default=None
            An identifier for the job
        k : int | None, default=10
            The number of clusters
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : int | None, default=10
            The maximum number of iterations
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        number_of_restarts : int | None, default=1
            The number of times the algorithm should be restarted with different initial centers
        random_seed : int | None, default=None
            Random seed for reproducible results
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        seed_centroids : Optional[list[list[float]]], default=None
            Initial centroids for the algorithm
        sudo : bool | None, default=False
            Override memory estimation limits
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
        G : GraphV2
            The graph to run the algorithm on.
        node_property : str
            The node property to use for clustering
        compute_silhouette : bool | None, default=False
            Whether to compute silhouette coefficient
        concurrency : int | None, default=None
            The number of concurrent threads
        delta_threshold : float | None, default=0.05
            The convergence threshold for the algorithm
        initial_sampler : str | None, default="UNIFORM"
            The sampling method for initial centroids
        job_id : str | None, default=None
            An identifier for the job
        k : int | None, default=10
            The number of clusters
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : int | None, default=10
            The maximum number of iterations
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        number_of_restarts : int | None, default=1
            The number of times the algorithm should be restarted with different initial centers
        random_seed : int | None, default=None
            Random seed for reproducible results
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        seed_centroids : Optional[list[list[float]]], default=None
            Initial centroids for the algorithm
        sudo : bool | None, default=False
            Override memory estimation limits
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
        G : GraphV2
            The graph to run the algorithm on.
        node_property : str
            The node property to use for clustering
        compute_silhouette : bool | None, default=False
            Whether to compute silhouette coefficient
        concurrency : int | None, default=None
            The number of concurrent threads
        delta_threshold : float | None, default=0.05
            The convergence threshold for the algorithm
        initial_sampler : str | None, default="UNIFORM"
            The sampling method for initial centroids
        job_id : str | None, default=None
            An identifier for the job
        k : int | None, default=10
            The number of clusters
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : int | None, default=10
            The maximum number of iterations
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        number_of_restarts : int | None, default=1
            The number of times the algorithm should be restarted with different initial centers
        random_seed : int | None, default=None
            Random seed for reproducible results
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        seed_centroids : Optional[list[list[float]]], default=None
            Initial centroids for the algorithm
        sudo : bool | None, default=False
            Override memory estimation limits
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
        G : GraphV2
            The graph to run the algorithm on.
        node_property : str
            The node property to use for clustering
        write_property : str
            The property name to write the community IDs to
        compute_silhouette : bool | None, default=False
            Whether to compute silhouette coefficient
        concurrency : int | None, default=None
            The number of concurrent threads
        delta_threshold : float | None, default=0.05
            The convergence threshold for the algorithm
        initial_sampler : str | None, default="UNIFORM"
            The sampling method for initial centroids
        job_id : str | None, default=None
            An identifier for the job
        k : int | None, default=10
            The number of clusters
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : int | None, default=10
            The maximum number of iterations
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        number_of_restarts : int | None, default=1
            The number of times the algorithm should be restarted with different initial centers
        random_seed : int | None, default=None
            Random seed for reproducible results
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        seed_centroids : Optional[list[list[float]]], default=None
            Initial centroids for the algorithm
        sudo : bool | None, default=False
            Override memory estimation limits
        username : str | None, default=None
            The username to attribute the procedure run to
        write_concurrency : int | None, default=None
            The number of concurrent threads for write operations

        Returns
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
        concurrency : int | None, default=None
            The number of concurrent threads
        delta_threshold : float | None, default=0.05
            The convergence threshold for the algorithm
        initial_sampler : str | None, default="UNIFORM"
            The sampling method for initial centroids
        k : int | None, default=10
            The number of clusters
        max_iterations : int | None, default=10
            The maximum number of iterations
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        number_of_restarts : int | None, default=1
            The number of times the algorithm should be restarted with different initial centers
        random_seed : int | None, default=None
            Random seed for reproducible results
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
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
