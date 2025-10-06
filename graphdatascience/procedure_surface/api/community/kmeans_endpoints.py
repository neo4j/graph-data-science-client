from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class KMeansEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        node_property: str,
        mutate_property: str,
        *,
        compute_silhouette: Optional[bool] = False,
        concurrency: Optional[int] = 4,
        delta_threshold: Optional[float] = 0.05,
        initial_sampler: Optional[str] = "UNIFORM",
        job_id: Optional[str] = None,
        k: Optional[int] = 10,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        number_of_restarts: Optional[int] = 1,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        seed_centroids: Optional[List[List[float]]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> KMeansMutateResult:
        """
        Executes the K-Means algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        node_property : str
            The node property to use for clustering
        mutate_property : str
            The property name to store the community ID for each node
        compute_silhouette : Optional[bool], default=False
            Whether to compute silhouette coefficient
        concurrency : Optional[int], default=4
            The number of concurrent threads
        delta_threshold : Optional[float], default=0.05
            The convergence threshold for the algorithm
        initial_sampler : Optional[str], default="UNIFORM"
            The sampling method for initial centroids
        job_id : Optional[str], default=None
            An identifier for the job
        k : Optional[int], default=10
            The number of clusters
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : Optional[int], default=10
            The maximum number of iterations
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        number_of_restarts : Optional[int], default=1
            The number of times the algorithm should be restarted with different initial centers
        random_seed : Optional[int], default=None
            Random seed for reproducible results
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        seed_centroids : Optional[List[List[float]]], default=None
            Initial centroids for the algorithm
        sudo : Optional[bool], default=False
            Override memory estimation limits
        username : Optional[str], default=None
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
        compute_silhouette: Optional[bool] = False,
        concurrency: Optional[int] = 4,
        delta_threshold: Optional[float] = 0.05,
        initial_sampler: Optional[str] = "UNIFORM",
        job_id: Optional[str] = None,
        k: Optional[int] = 10,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        number_of_restarts: Optional[int] = 1,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        seed_centroids: Optional[List[List[float]]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> KMeansStatsResult:
        """
        Executes the K-Means algorithm and returns statistics.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        node_property : str
            The node property to use for clustering
        compute_silhouette : Optional[bool], default=False
            Whether to compute silhouette coefficient
        concurrency : Optional[int], default=4
            The number of concurrent threads
        delta_threshold : Optional[float], default=0.05
            The convergence threshold for the algorithm
        initial_sampler : Optional[str], default="UNIFORM"
            The sampling method for initial centroids
        job_id : Optional[str], default=None
            An identifier for the job
        k : Optional[int], default=10
            The number of clusters
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : Optional[int], default=10
            The maximum number of iterations
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        number_of_restarts : Optional[int], default=1
            The number of times the algorithm should be restarted with different initial centers
        random_seed : Optional[int], default=None
            Random seed for reproducible results
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        seed_centroids : Optional[List[List[float]]], default=None
            Initial centroids for the algorithm
        sudo : Optional[bool], default=False
            Override memory estimation limits
        username : Optional[str], default=None
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
        compute_silhouette: Optional[bool] = False,
        concurrency: Optional[int] = 4,
        delta_threshold: Optional[float] = 0.05,
        initial_sampler: Optional[str] = "UNIFORM",
        job_id: Optional[str] = None,
        k: Optional[int] = 10,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        number_of_restarts: Optional[int] = 1,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        seed_centroids: Optional[List[List[float]]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> DataFrame:
        """
        Executes the K-Means algorithm and returns a stream of results.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        node_property : str
            The node property to use for clustering
        compute_silhouette : Optional[bool], default=False
            Whether to compute silhouette coefficient
        concurrency : Optional[int], default=4
            The number of concurrent threads
        delta_threshold : Optional[float], default=0.05
            The convergence threshold for the algorithm
        initial_sampler : Optional[str], default="UNIFORM"
            The sampling method for initial centroids
        job_id : Optional[str], default=None
            An identifier for the job
        k : Optional[int], default=10
            The number of clusters
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : Optional[int], default=10
            The maximum number of iterations
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        number_of_restarts : Optional[int], default=1
            The number of times the algorithm should be restarted with different initial centers
        random_seed : Optional[int], default=None
            Random seed for reproducible results
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        seed_centroids : Optional[List[List[float]]], default=None
            Initial centroids for the algorithm
        sudo : Optional[bool], default=False
            Override memory estimation limits
        username : Optional[str], default=None
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
        compute_silhouette: Optional[bool] = False,
        concurrency: Optional[int] = 4,
        delta_threshold: Optional[float] = 0.05,
        initial_sampler: Optional[str] = "UNIFORM",
        job_id: Optional[str] = None,
        k: Optional[int] = 10,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        number_of_restarts: Optional[int] = 1,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        seed_centroids: Optional[List[List[float]]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
        write_concurrency: Optional[int] = None,
    ) -> KMeansWriteResult:
        """
        Executes the K-Means algorithm and writes the results back to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        node_property : str
            The node property to use for clustering
        write_property : str
            The property name to write the community IDs to
        compute_silhouette : Optional[bool], default=False
            Whether to compute silhouette coefficient
        concurrency : Optional[int], default=4
            The number of concurrent threads
        delta_threshold : Optional[float], default=0.05
            The convergence threshold for the algorithm
        initial_sampler : Optional[str], default="UNIFORM"
            The sampling method for initial centroids
        job_id : Optional[str], default=None
            An identifier for the job
        k : Optional[int], default=10
            The number of clusters
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : Optional[int], default=10
            The maximum number of iterations
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        number_of_restarts : Optional[int], default=1
            The number of times the algorithm should be restarted with different initial centers
        random_seed : Optional[int], default=None
            Random seed for reproducible results
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        seed_centroids : Optional[List[List[float]]], default=None
            Initial centroids for the algorithm
        sudo : Optional[bool], default=False
            Override memory estimation limits
        username : Optional[str], default=None
            The username to attribute the procedure run to
        write_concurrency : Optional[int], default=None
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
        G: Union[GraphV2, dict[str, Any]],
        node_property: str,
        *,
        compute_silhouette: Optional[bool] = False,
        concurrency: Optional[int] = 4,
        delta_threshold: Optional[float] = 0.05,
        initial_sampler: Optional[str] = "UNIFORM",
        k: Optional[int] = 10,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        number_of_restarts: Optional[int] = 1,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        seed_centroids: Optional[List[List[float]]] = None,
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the K-Means algorithm.

        Parameters
        ----------
        G : Union[GraphV2, dict[str, Any]]
            The graph or graph configuration to estimate for
        node_property : str
            The node property to use for clustering
        compute_silhouette : Optional[bool], default=False
            Whether to compute silhouette coefficient
        concurrency : Optional[int], default=4
            The number of concurrent threads
        delta_threshold : Optional[float], default=0.05
            The convergence threshold for the algorithm
        initial_sampler : Optional[str], default="UNIFORM"
            The sampling method for initial centroids
        k : Optional[int], default=10
            The number of clusters
        max_iterations : Optional[int], default=10
            The maximum number of iterations
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        number_of_restarts : Optional[int], default=1
            The number of times the algorithm should be restarted with different initial centers
        random_seed : Optional[int], default=None
            Random seed for reproducible results
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        seed_centroids : Optional[List[List[float]]], default=None
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
    centroids: List[Any]
    community_distribution: dict[str, Any]
    compute_millis: int
    configuration: dict[str, Any]
    mutate_millis: int
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int


class KMeansStatsResult(BaseResult):
    average_distance_to_centroid: float
    average_silhouette: float
    centroids: List[Any]
    community_distribution: dict[str, Any]
    compute_millis: int
    configuration: dict[str, Any]
    post_processing_millis: int
    pre_processing_millis: int


class KMeansWriteResult(BaseResult):
    average_distance_to_centroid: float
    average_silhouette: float
    centroids: List[Any]
    community_distribution: dict[str, Any]
    compute_millis: int
    configuration: dict[str, Any]
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int
    write_millis: int
