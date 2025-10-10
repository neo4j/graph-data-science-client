from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class EigenvectorEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        max_iterations: int | None = None,
        tolerance: float | None = None,
        source_nodes: Any | None = None,
        scaler: Any | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> EigenvectorMutateResult:
        """
        Runs the Eigenvector Centrality algorithm and stores the results in the graph catalog as a new node property.

        Eigenvector Centrality is an algorithm that measures the transitive influence of nodes.
        Relationships originating from high-scoring nodes contribute more to the score of a node than connections from low-scoring nodes.
        A high eigenvector score means that a node is connected to many nodes who themselves have high scores.
        The algorithm computes the eigenvector associated with the largest absolute eigenvalue.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the eigenvector centrality score for each node
        max_iterations : int | None, default=None
            The maximum number of iterations to run the algorithm
        tolerance : float | None, default=None
            The tolerance for convergence detection
        source_nodes : Any | None, default=None
            The source nodes to start the computation from
        scaler : Any | None, default=None
            Scaling configuration for the algorithm
        relationship_weight_property : str | None, default=None
            The property name that contains weight values for relationships
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run.
        sudo : bool | None, default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : bool | None, default=None
            Whether to log progress of the algorithm execution
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            The number of concurrent threads used for the algorithm execution.
        job_id : Any | None, default=None
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
        max_iterations: int | None = None,
        tolerance: float | None = None,
        source_nodes: Any | None = None,
        scaler: Any | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> EigenvectorStatsResult:
        """
        Runs the Eigenvector Centrality algorithm and returns result statistics without storing the results.

        Eigenvector Centrality is an algorithm that measures the transitive influence of nodes.
        Relationships originating from high-scoring nodes contribute more to the score of a node than connections from low-scoring nodes.
        A high eigenvector score means that a node is connected to many nodes who themselves have high scores.
        The algorithm computes the eigenvector associated with the largest absolute eigenvalue.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        max_iterations : int | None, default=None
            The maximum number of iterations to run the algorithm
        tolerance : float | None, default=None
            The tolerance for convergence detection
        source_nodes : Any | None, default=None
            The source nodes to start the computation from
        scaler : Any | None, default=None
            Scaling configuration for the algorithm
        relationship_weight_property : str | None, default=None
            The property name that contains weight values for relationships
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run.
        sudo : bool | None, default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : bool | None, default=None
            Whether to log progress of the algorithm execution
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            The number of concurrent threads used for the algorithm execution.
        job_id : Any | None, default=None
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
        max_iterations: int | None = None,
        tolerance: float | None = None,
        source_nodes: Any | None = None,
        scaler: Any | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> DataFrame:
        """
        Executes the Eigenvector Centrality algorithm and returns a stream of results.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        max_iterations : int | None, default=None
            The maximum number of iterations to run the algorithm
        tolerance : float | None, default=None
            The tolerance for convergence detection
        source_nodes : Any | None, default=None
            The source nodes to start the computation from
        scaler : Any | None, default=None
            Scaling configuration for the algorithm
        relationship_weight_property : str | None, default=None
            The property name that contains weight values for relationships
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run.
        sudo : bool | None, default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : bool | None, default=None
            Whether to log progress of the algorithm execution
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            The number of concurrent threads used for the algorithm execution.
        job_id : Any | None, default=None
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
        max_iterations: int | None = None,
        tolerance: float | None = None,
        source_nodes: Any | None = None,
        scaler: Any | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        write_concurrency: Any | None = None,
    ) -> EigenvectorWriteResult:
        """
        Runs the Eigenvector Centrality algorithm and stores the result in the Neo4j database as a new node property.

        Eigenvector Centrality is an algorithm that measures the transitive influence of nodes.
        Relationships originating from high-scoring nodes contribute more to the score of a node than connections from low-scoring nodes.
        A high eigenvector score means that a node is connected to many nodes who themselves have high scores.
        The algorithm computes the eigenvector associated with the largest absolute eigenvalue.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to write the eigenvector centrality scores to
        max_iterations : int | None, default=None
            The maximum number of iterations to run the algorithm
        tolerance : float | None, default=None
            The tolerance for convergence detection
        source_nodes : Any | None, default=None
            The source nodes to start the computation from
        scaler : Any | None, default=None
            Scaling configuration for the algorithm
        relationship_weight_property : str | None, default=None
            The property name that contains weight values for relationships
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run.
        sudo : bool | None, default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : bool | None, default=None
            Whether to log progress of the algorithm execution
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            The number of concurrent threads used for the algorithm execution.
        job_id : Any | None, default=None
            An identifier for the job that can be used for monitoring and cancellation
        write_concurrency : Any | None, default=None
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
        max_iterations: int | None = None,
        tolerance: float | None = None,
        source_nodes: Any | None = None,
        scaler: Any | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on or a dictionary representing the graph configuration.
        max_iterations : int | None, default=None
            The maximum number of iterations to run the algorithm
        tolerance : float | None, default=None
            The tolerance for convergence detection
        source_nodes : Any | None, default=None
            The source nodes to start the computation from
        scaler : Any | None, default=None
            Scaling configuration for the algorithm
        relationship_weight_property : str | None, default=None
            The property name that contains weight values for relationships
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run.
        concurrency : Any | None, default=None
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
