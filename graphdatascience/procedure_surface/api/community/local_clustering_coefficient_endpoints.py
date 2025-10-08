from abc import abstractmethod
from typing import Any, Dict, List, Optional

import pandas as pd

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
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
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        triangle_count_property: Optional[str] = None,
        username: Optional[str] = None,
    ) -> "LocalClusteringCoefficientMutateResult":
        """
        Executes the LocalClusteringCoefficient algorithm and writes results back to the graph.

        Parameters
        ----------
        G : GraphV2
            The graph on which to run the algorithm
        mutate_property : str
            Property name to store the result
        concurrency : Optional[int], default=None
            Number of concurrent threads
        job_id : Optional[str], default=None
            Job identifier for tracking
        log_progress : bool, default=True
            Whether to log progress
        node_labels : Optional[List[str]], default=None
            Node labels to include in the computation
        relationship_types : Optional[List[str]], default=None
            Relationship types to include in the computation
        sudo : Optional[bool], default=False
            Run with elevated privileges
        triangle_count_property : Optional[str], default=None
            Property name for pre-computed triangle counts
        username : Optional[str], default=None
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
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        triangle_count_property: Optional[str] = None,
        username: Optional[str] = None,
    ) -> "LocalClusteringCoefficientStatsResult":
        """
        Executes the LocalClusteringCoefficient algorithm and returns statistics.

        Parameters
        ----------
        G : GraphV2
            The graph on which to run the algorithm
        concurrency : Optional[int], default=None
            Number of concurrent threads
        job_id : Optional[str], default=None
            Job identifier for tracking
        log_progress : bool, default=True
            Whether to log progress
        node_labels : Optional[List[str]], default=None
            Node labels to include in the computation
        relationship_types : Optional[List[str]], default=None
            Relationship types to include in the computation
        sudo : Optional[bool], default=False
            Run with elevated privileges
        triangle_count_property : Optional[str], default=None
            Property name for pre-computed triangle counts
        username : Optional[str], default=None
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
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        triangle_count_property: Optional[str] = None,
        username: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Executes the LocalClusteringCoefficient algorithm and streams results.

        Parameters
        ----------
        G : GraphV2
            The graph on which to run the algorithm
        concurrency : Optional[int], default=None
            Number of concurrent threads
        job_id : Optional[str], default=None
            Job identifier for tracking
        log_progress : bool, default=True
            Whether to log progress
        node_labels : Optional[List[str]], default=None
            Node labels to include in the computation
        relationship_types : Optional[List[str]], default=None
            Relationship types to include in the computation
        sudo : Optional[bool], default=False
            Run with elevated privileges
        triangle_count_property : Optional[str], default=None
            Property name for pre-computed triangle counts
        username : Optional[str], default=None
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
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        triangle_count_property: Optional[str] = None,
        username: Optional[str] = None,
        write_concurrency: Optional[int] = None,
        write_to_result_store: Optional[bool] = None,
    ) -> "LocalClusteringCoefficientWriteResult":
        """
        Executes the LocalClusteringCoefficient algorithm and writes results to the database.

        Parameters
        ----------
        G : GraphV2
            The graph on which to run the algorithm
        write_property : str
            Property name to store results in the database
        concurrency : Optional[int], default=None
            Number of concurrent threads
        job_id : Optional[str], default=None
            Job identifier for tracking
        log_progress : bool, default=True
            Whether to log progress
        node_labels : Optional[List[str]], default=None
            Node labels to include in the computation
        relationship_types : Optional[List[str]], default=None
            Relationship types to include in the computation
        sudo : Optional[bool], default=False
            Run with elevated privileges
        triangle_count_property : Optional[str], default=None
            Property name for pre-computed triangle counts
        username : Optional[str], default=None
            Username for authentication
        write_concurrency : Optional[int], default=None
            Concurrency for writing back to the database
        write_to_result_store : Optional[bool], default=None
            Whether to write to the result store

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
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        triangle_count_property: Optional[str] = None,
        username: Optional[str] = None,
    ) -> EstimationResult:
        """
        Estimates the LocalClusteringCoefficient algorithm memory requirements.

        Parameters
        ----------
        G : GraphV2
            The graph on which to run the algorithm
        concurrency : Optional[int], default=None
            Number of concurrent threads
        job_id : Optional[str], default=None
            Job identifier for tracking
        log_progress : bool, default=True
            Whether to log progress
        node_labels : Optional[List[str]], default=None
            Node labels to include in the computation
        relationship_types : Optional[List[str]], default=None
            Relationship types to include in the computation
        sudo : Optional[bool], default=False
            Run with elevated privileges
        triangle_count_property : Optional[str], default=None
            Property name for pre-computed triangle counts
        username : Optional[str], default=None
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
    configuration: Dict[str, Any]


class LocalClusteringCoefficientStatsResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    node_count: int
    average_clustering_coefficient: float
    configuration: Dict[str, Any]


class LocalClusteringCoefficientWriteResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    node_count: int
    node_properties_written: int
    average_clustering_coefficient: float
    configuration: Dict[str, Any]
