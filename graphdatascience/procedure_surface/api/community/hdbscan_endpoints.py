from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class HdbscanEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        node_property: str,
        mutate_property: str,
        *,
        leaf_size: int = 1,
        samples: int = 10,
        min_cluster_size: int = 5,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        job_id: str | None = None,
        username: str | None = None,
    ) -> HdbscanMutateResult:
        """
        Runs the HDBSCAN algorithm and writes the cluster ID for each node back to the in-memory graph.

        The algorithm performs hierarchical density-based clustering on a node property,
        identifying clusters based on density reachability.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        node_property : str
            The node property to use for clustering (required)
        mutate_property : str
            The name of the node property to write the cluster ID to
        leaf_size : int | None, default=None
            The maximum leaf size of the tree structure used in the algorithm
        samples : int | None, default=None
            The number of samples used for density estimation
        min_cluster_size : int | None, default=None
            The minimum size of clusters
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency : int | None, default=None
            The number of concurrent threads
        log_progress : bool, default=True
            Whether to log progress
        sudo
            Disable the memory guard.
        job_id : str | None
            An identifier for the job
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        HdbscanMutateResult
            The result containing statistics about the clustering and algorithm execution
        """

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        node_property: str,
        *,
        leaf_size: int = 1,
        samples: int = 10,
        min_cluster_size: int = 5,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        job_id: str | None = None,
        username: str | None = None,
    ) -> HdbscanStatsResult:
        """
        Runs the HDBSCAN algorithm and returns only statistics about the clustering.

        This mode computes cluster assignments without writing them back to the graph,
        returning only execution statistics and cluster information.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        node_property : str
            The node property to use for clustering (required)
        leaf_size : int | None, default=None
            The maximum leaf size of the tree structure used in the algorithm
        samples : int | None, default=None
            The number of samples used for density estimation
        min_cluster_size : int | None, default=None
            The minimum size of clusters
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency : int | None, default=None
            The number of concurrent threads
        log_progress : bool, default=True
            Whether to log progress
        sudo
            Disable the memory guard.
        job_id : str | None
            An identifier for the job
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        HdbscanStatsResult
            The result containing statistics about the clustering and algorithm execution
        """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        node_property: str,
        *,
        leaf_size: int = 1,
        samples: int = 10,
        min_cluster_size: int = 5,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        job_id: str | None = None,
        username: str | None = None,
    ) -> DataFrame:
        """
        Runs the HDBSCAN algorithm and returns the cluster ID for each node as a DataFrame.

        The DataFrame contains the cluster assignment for each node, with noise points
        typically assigned to cluster -1.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        node_property : str
            The node property to use for clustering (required)
        leaf_size : int | None, default=None
            The maximum leaf size of the tree structure used in the algorithm
        samples : int | None, default=None
            The number of samples used for density estimation
        min_cluster_size : int | None, default=None
            The minimum size of clusters
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency : int | None, default=None
            The number of concurrent threads
        log_progress : bool, default=True
            Whether to log progress
        sudo
            Disable the memory guard.
        job_id : str | None
            An identifier for the job
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        pd.DataFrame
            A DataFrame with columns 'nodeId' and 'label'
        """

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        node_property: str,
        write_property: str,
        *,
        leaf_size: int = 1,
        samples: int = 10,
        min_cluster_size: int = 5,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        job_id: str | None = None,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> HdbscanWriteResult:
        """
        Runs the HDBSCAN algorithm and writes the cluster ID for each node back to the database.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        node_property : str
            The node property to use for clustering (required)
        write_property : str
            The name of the node property to write the cluster ID to
        leaf_size : int | None, default=None
            The maximum leaf size of the tree structure used in the algorithm
        samples : int | None, default=None
            The number of samples used for density estimation
        min_cluster_size : int | None, default=None
            The minimum size of clusters
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        write_concurrency : int | None, default=None
            The number of concurrent threads for writing
        concurrency : int | None, default=None
            The number of concurrent threads
        log_progress : bool, default=True
            Whether to log progress
        sudo
            Disable the memory guard.
        job_id : str | None
            An identifier for the job
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        HdbscanWriteResult
            The result containing statistics about the clustering and algorithm execution
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        node_property: str,
        *,
        leaf_size: int = 1,
        samples: int = 10,
        min_cluster_size: int = 5,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """
        Estimates memory requirements and other statistics for the HDBSCAN algorithm.

        This method provides memory estimation for the HDBSCAN algorithm without
        actually executing the clustering. It helps determine the computational requirements
        before running the actual clustering procedure.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        node_property : str
            The node property to use for clustering (required)
        leaf_size : int | None, default=None
            The maximum leaf size of the tree structure used in the algorithm
        samples : int | None, default=None
            The number of samples used for density estimation
        min_cluster_size : int | None, default=None
            The minimum size of clusters
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency : int | None, default=None
            The number of concurrent threads
        log_progress : bool, default=True
            Whether to log progress
        sudo
            Disable the memory guard.
        job_id : str | None
            An identifier for the job
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        EstimationResult
            The estimation result containing memory requirements and other statistics
        """


class HdbscanMutateResult(BaseResult):
    compute_millis: int
    configuration: dict[str, Any]
    mutate_millis: int
    node_count: int
    node_properties_written: int
    number_of_clusters: int
    number_of_noise_points: int
    post_processing_millis: int
    pre_processing_millis: int


class HdbscanStatsResult(BaseResult):
    compute_millis: int
    configuration: dict[str, Any]
    node_count: int
    number_of_clusters: int
    number_of_noise_points: int
    post_processing_millis: int
    pre_processing_millis: int


class HdbscanWriteResult(BaseResult):
    compute_millis: int
    configuration: dict[str, Any]
    node_count: int
    node_properties_written: int
    number_of_clusters: int
    number_of_noise_points: int
    post_processing_millis: int
    pre_processing_millis: int
    write_millis: int
