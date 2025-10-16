from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class HdbscanEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        node_property: str,
        mutate_property: str,
        *,
        leaf_size: int | None = None,
        samples: int | None = None,
        min_cluster_size: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        job_id: Any | None = None,
        username: str | None = None,
    ) -> HdbscanMutateResult:
        """
        Runs the HDBSCAN algorithm and writes the cluster ID for each node back to the in-memory graph.

        The algorithm performs hierarchical density-based clustering on a node property,
        identifying clusters based on density reachability.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
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
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        concurrency : int | None, default=None
            The number of concurrent threads
        log_progress : bool, default=True
            Whether to log progress
        sudo : bool | None, default=None
            Override memory estimation limits
        job_id : Any | None, default=None
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
        leaf_size: int | None = None,
        samples: int | None = None,
        min_cluster_size: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        job_id: Any | None = None,
        username: str | None = None,
    ) -> HdbscanStatsResult:
        """
        Runs the HDBSCAN algorithm and returns only statistics about the clustering.

        This mode computes cluster assignments without writing them back to the graph,
        returning only execution statistics and cluster information.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        node_property : str
            The node property to use for clustering (required)
        leaf_size : int | None, default=None
            The maximum leaf size of the tree structure used in the algorithm
        samples : int | None, default=None
            The number of samples used for density estimation
        min_cluster_size : int | None, default=None
            The minimum size of clusters
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        concurrency : int | None, default=None
            The number of concurrent threads
        log_progress : bool, default=True
            Whether to log progress
        sudo : bool | None, default=None
            Override memory estimation limits
        job_id : Any | None, default=None
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
        leaf_size: int | None = None,
        samples: int | None = None,
        min_cluster_size: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        job_id: Any | None = None,
        username: str | None = None,
    ) -> pd.DataFrame:
        """
        Runs the HDBSCAN algorithm and returns the cluster ID for each node as a DataFrame.

        The DataFrame contains the cluster assignment for each node, with noise points
        typically assigned to cluster -1.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        node_property : str
            The node property to use for clustering (required)
        leaf_size : int | None, default=None
            The maximum leaf size of the tree structure used in the algorithm
        samples : int | None, default=None
            The number of samples used for density estimation
        min_cluster_size : int | None, default=None
            The minimum size of clusters
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        concurrency : int | None, default=None
            The number of concurrent threads
        log_progress : bool, default=True
            Whether to log progress
        sudo : bool | None, default=None
            Override memory estimation limits
        job_id : Any | None, default=None
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
        leaf_size: int | None = None,
        samples: int | None = None,
        min_cluster_size: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        write_concurrency: int | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        job_id: Any | None = None,
        username: str | None = None,
    ) -> HdbscanWriteResult:
        """
        Runs the HDBSCAN algorithm and writes the cluster ID for each node back to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
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
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        write_concurrency : int | None, default=None
            The number of concurrent threads for writing
        concurrency : int | None, default=None
            The number of concurrent threads
        log_progress : bool, default=True
            Whether to log progress
        sudo : bool | None, default=None
            Override memory estimation limits
        job_id : Any | None, default=None
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
        G: GraphV2,
        node_property: str,
        *,
        leaf_size: int | None = None,
        samples: int | None = None,
        min_cluster_size: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        job_id: Any | None = None,
        username: str | None = None,
    ) -> EstimationResult:
        """
        Estimates memory requirements and other statistics for the HDBSCAN algorithm.

        This method provides memory estimation for the HDBSCAN algorithm without
        actually executing the clustering. It helps determine the computational requirements
        before running the actual clustering procedure.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        node_property : str
            The node property to use for clustering (required)
        leaf_size : int | None, default=None
            The maximum leaf size of the tree structure used in the algorithm
        samples : int | None, default=None
            The number of samples used for density estimation
        min_cluster_size : int | None, default=None
            The minimum size of clusters
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        concurrency : int | None, default=None
            The number of concurrent threads
        log_progress : bool, default=True
            Whether to log progress
        sudo : bool | None, default=None
            Override memory estimation limits
        job_id : Any | None, default=None
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
