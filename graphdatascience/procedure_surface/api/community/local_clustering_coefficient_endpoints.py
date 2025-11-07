from abc import abstractmethod
from typing import Any

import pandas as pd

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class LocalClusteringCoefficientEndpoints:
    """
    Interface for LocalClusteringCoefficient algorithm endpoints.
    """

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        *,
        mutate_property: str,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool | None = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
    ) -> "LocalClusteringCoefficientMutateResult":
        """
        Executes the LocalClusteringCoefficient algorithm and writes results back to the graph.

        Parameters
        ----------
        G : GraphV2
            The graph on which to run the algorithm
        mutate_property : str
            Property name to store the result
        concurrency : int | None, default=None
            Number of concurrent threads
        job_id : str | None, default=None
            Job identifier for tracking
        log_progress : bool, default=True
            Whether to log progress
        node_labels : list[str] | None, default=None
            Node labels to include in the computation
        relationship_types : list[str] | None, default=None
            Relationship types to include in the computation
        sudo : bool | None, default=False
            Run with elevated privileges
        triangle_count_property : str | None, default=None
            Property name for pre-computed triangle counts
        username : str | None, default=None
            Username for authentication

        Returns
        -------
        LocalClusteringCoefficientMutateResult
            Result containing clustering coefficient statistics and timing information
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool | None = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
    ) -> "LocalClusteringCoefficientStatsResult":
        """
        Executes the LocalClusteringCoefficient algorithm and returns statistics.

        Parameters
        ----------
        G : GraphV2
            The graph on which to run the algorithm
        concurrency : int | None, default=None
            Number of concurrent threads
        job_id : str | None, default=None
            Job identifier for tracking
        log_progress : bool, default=True
            Whether to log progress
        node_labels : list[str] | None, default=None
            Node labels to include in the computation
        relationship_types : list[str] | None, default=None
            Relationship types to include in the computation
        sudo : bool | None, default=False
            Run with elevated privileges
        triangle_count_property : str | None, default=None
            Property name for pre-computed triangle counts
        username : str | None, default=None
            Username for authentication

        Returns
        -------
        LocalClusteringCoefficientStatsResult
            Result containing clustering coefficient statistics and timing information
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool | None = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
    ) -> pd.DataFrame:
        """
        Executes the LocalClusteringCoefficient algorithm and streams results.

        Parameters
        ----------
        G : GraphV2
            The graph on which to run the algorithm
        concurrency : int | None, default=None
            Number of concurrent threads
        job_id : str | None, default=None
            Job identifier for tracking
        log_progress : bool, default=True
            Whether to log progress
        node_labels : list[str] | None, default=None
            Node labels to include in the computation
        relationship_types : list[str] | None, default=None
            Relationship types to include in the computation
        sudo : bool | None, default=False
            Run with elevated privileges
        triangle_count_property : str | None, default=None
            Property name for pre-computed triangle counts
        username : str | None, default=None
            Username for authentication

        Returns
        -------
        pandas.DataFrame
            DataFrame containing nodeId and localClusteringCoefficient columns
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        *,
        write_property: str,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool | None = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> "LocalClusteringCoefficientWriteResult":
        """
        Executes the LocalClusteringCoefficient algorithm and writes results to the database.

        Parameters
        ----------
        G : GraphV2
            The graph on which to run the algorithm
        write_property : str
            Property name to store results in the database
        concurrency : int | None, default=None
            Number of concurrent threads
        job_id : str | None, default=None
            Job identifier for tracking
        log_progress : bool, default=True
            Whether to log progress
        node_labels : list[str] | None, default=None
            Node labels to include in the computation
        relationship_types : list[str] | None, default=None
            Relationship types to include in the computation
        sudo : bool | None, default=False
            Run with elevated privileges
        triangle_count_property : str | None, default=None
            Property name for pre-computed triangle counts
        username : str | None, default=None
            Username for authentication
        write_concurrency : int | None, default=None
            Concurrency for writing back to the database

        Returns
        -------
        LocalClusteringCoefficientWriteResult
            Result containing clustering coefficient statistics and timing information
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool | None = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
    ) -> EstimationResult:
        """
        Estimates the LocalClusteringCoefficient algorithm memory requirements.

        Parameters
        ----------
        G : GraphV2
            The graph on which to run the algorithm
        concurrency : int | None, default=None
            Number of concurrent threads
        job_id : str | None, default=None
            Job identifier for tracking
        log_progress : bool, default=True
            Whether to log progress
        node_labels : list[str] | None, default=None
            Node labels to include in the computation
        relationship_types : list[str] | None, default=None
            Relationship types to include in the computation
        sudo : bool | None, default=False
            Run with elevated privileges
        triangle_count_property : str | None, default=None
            Property name for pre-computed triangle counts
        username : str | None, default=None
            Username for authentication

        Returns
        -------
        EstimationResult
            Memory estimation details
        """
        pass


class LocalClusteringCoefficientMutateResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    node_count: int
    node_properties_written: int
    average_clustering_coefficient: float
    configuration: dict[str, Any]


class LocalClusteringCoefficientStatsResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    node_count: int
    average_clustering_coefficient: float
    configuration: dict[str, Any]


class LocalClusteringCoefficientWriteResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    node_count: int
    node_properties_written: int
    average_clustering_coefficient: float
    configuration: dict[str, Any]
