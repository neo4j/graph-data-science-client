from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.scaler_config import ScalerConfig
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class EigenvectorEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        max_iterations: int = 20,
        tolerance: float = 1.0e-7,
        source_nodes: int | list[int] | None = None,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> EigenvectorMutateResult:
        """
        Runs the Eigenvector Centrality algorithm and stores the results in the graph catalog as a new node property.

        Eigenvector Centrality is an algorithm that measures the transitive influence of nodes.
        Relationships originating from high-scoring nodes contribute more to the score of a node than connections from low-scoring nodes.
        A high eigenvector score means that a node is connected to many nodes who themselves have high scores.
        The algorithm computes the eigenvector associated with the largest absolute eigenvalue.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        mutate_property : str
            The property name to store the eigenvector centrality score for each node
        max_iterations : int
            The maximum number of iterations to run the algorithm
        tolerance : float
            The tolerance for convergence detection
        source_nodes : int | list[int] | list[tuple[int, float]] | None, default=None
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
        scaler : str | dict[str, str | int | float] | ScalerConfig, default="NONE"
            The scaler to use. Can be:
            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center', 'NONE')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - A ScalerConfig instance
            - "NONE" (default, no scaling)
        relationship_weight_property : str | None, default=None
            The property name that contains weight values for relationships
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run.
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run.
        sudo : bool
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : bool, default=True
            Whether to log progress of the algorithm execution
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            The number of concurrent threads used for the algorithm execution.
        job_id : str | None
            An identifier for the job that can be used for monitoring and cancellation

        Returns
        -------
        EigenvectorMutateResult
            Algorithm metrics and statistics including the centrality distribution
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        max_iterations: int = 20,
        tolerance: float = 1.0e-7,
        source_nodes: int | list[int] | None = None,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> EigenvectorStatsResult:
        """
        Runs the Eigenvector Centrality algorithm and returns result statistics without storing the results.

        Eigenvector Centrality is an algorithm that measures the transitive influence of nodes.
        Relationships originating from high-scoring nodes contribute more to the score of a node than connections from low-scoring nodes.
        A high eigenvector score means that a node is connected to many nodes who themselves have high scores.
        The algorithm computes the eigenvector associated with the largest absolute eigenvalue.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        max_iterations : int
            The maximum number of iterations to run the algorithm
        tolerance : float
            The tolerance for convergence detection
        source_nodes : int | list[int] | list[tuple[int, float]] | None, default=None
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
        scaler : str | dict[str, str | int | float] | ScalerConfig, default="NONE"
            The scaler to use. Can be:
            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center', 'NONE')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - A ScalerConfig instance
            - "NONE" (default, no scaling)
        relationship_weight_property : str | None, default=None
            The property name that contains weight values for relationships
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run.
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run.
        sudo : bool
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : bool, default=True
            Whether to log progress of the algorithm execution
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            The number of concurrent threads used for the algorithm execution.
        job_id : str | None
            An identifier for the job that can be used for monitoring and cancellation

        Returns
        -------
        EigenvectorStatsResult
            Algorithm statistics including the centrality distribution
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        max_iterations: int = 20,
        tolerance: float = 1.0e-7,
        source_nodes: int | list[int] | None = None,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Executes the Eigenvector Centrality algorithm and returns a stream of results.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        max_iterations : int
            The maximum number of iterations to run the algorithm
        tolerance : float
            The tolerance for convergence detection
        source_nodes : int | list[int] | list[tuple[int, float]] | None, default=None
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
        scaler : str | dict[str, str | int | float] | ScalerConfig, default="NONE"
            The scaler to use. Can be:
            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center', 'NONE')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - A ScalerConfig instance
            - "NONE" (default, no scaling)
        relationship_weight_property : str | None, default=None
            The property name that contains weight values for relationships
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run.
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run.
        sudo : bool
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : bool, default=True
            Whether to log progress of the algorithm execution
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            The number of concurrent threads used for the algorithm execution.
        job_id : str | None
            An identifier for the job that can be used for monitoring and cancellation

        Returns
        -------
        DataFrame
            DataFrame with the algorithm results containing nodeId and score columns
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
        max_iterations: int = 20,
        tolerance: float = 1.0e-7,
        source_nodes: int | list[int] | None = None,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> EigenvectorWriteResult:
        """
        Runs the Eigenvector Centrality algorithm and stores the result in the Neo4j database as a new node property.

        Eigenvector Centrality is an algorithm that measures the transitive influence of nodes.
        Relationships originating from high-scoring nodes contribute more to the score of a node than connections from low-scoring nodes.
        A high eigenvector score means that a node is connected to many nodes who themselves have high scores.
        The algorithm computes the eigenvector associated with the largest absolute eigenvalue.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_property : str
            The property name to write the eigenvector centrality scores to
        max_iterations : int
            The maximum number of iterations to run the algorithm
        tolerance : float
            The tolerance for convergence detection
        source_nodes : int | list[int] | list[tuple[int, float]] | None, default=None
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
        scaler : str | dict[str, str | int | float] | ScalerConfig, default="NONE"
            The scaler to use. Can be:
            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center', 'NONE')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - A ScalerConfig instance
            - "NONE" (default, no scaling)
        relationship_weight_property : str | None, default=None
            The property name that contains weight values for relationships
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run.
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run.
        sudo : bool
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : bool, default=True
            Whether to log progress of the algorithm execution
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            The number of concurrent threads used for the algorithm execution.
        job_id : str | None
            An identifier for the job that can be used for monitoring and cancellation
        write_concurrency : int | None
            The number of concurrent threads during the write phase

        Returns
        -------
        EigenvectorWriteResult
            Algorithm metrics and statistics including the centrality distribution
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        max_iterations: int = 20,
        tolerance: float = 1.0e-7,
        source_nodes: int | list[int] | None = None,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on or a dictionary representing the graph configuration.
        max_iterations : int
            The maximum number of iterations to run the algorithm
        tolerance : float
            The tolerance for convergence detection
        source_nodes : int | list[int] | list[tuple[int, float]] | None, default=None
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
        scaler : str | dict[str, str | int | float] | ScalerConfig, default="NONE"
            The scaler to use. Can be:
            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center', 'NONE')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - A ScalerConfig instance
            - "NONE" (default, no scaling)
        relationship_weight_property : str | None, default=None
            The property name that contains weight values for relationships
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run.
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run.
        concurrency : int | None
            The number of concurrent threads used for the algorithm execution.

        Returns
        -------
        EstimationResult
            An object containing the result of the estimation
        """
        pass


class EigenvectorMutateResult(BaseResult):
    """Result of running Eigenvector Centrality algorithm with mutate mode."""

    node_properties_written: int
    ran_iterations: int
    did_converge: bool
    centrality_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    configuration: dict[str, Any]


class EigenvectorStatsResult(BaseResult):
    """Result of running Eigenvector Centrality algorithm with stats mode."""

    ran_iterations: int
    did_converge: bool
    centrality_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class EigenvectorWriteResult(BaseResult):
    """Result of running Eigenvector Centrality algorithm with write mode."""

    node_properties_written: int
    ran_iterations: int
    did_converge: bool
    centrality_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    configuration: dict[str, Any]
