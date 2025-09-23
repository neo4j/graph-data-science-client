from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

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
        max_iterations: Optional[int] = None,
        tolerance: Optional[float] = None,
        source_nodes: Optional[Any] = None,
        scaler: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
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
        max_iterations : Optional[int], default=None
            The maximum number of iterations to run the algorithm
        tolerance : Optional[float], default=None
            The tolerance for convergence detection
        source_nodes : Optional[Any], default=None
            The source nodes to start the computation from
        scaler : Optional[Any], default=None
            Scaling configuration for the algorithm
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight values for relationships
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : Optional[bool], default=None
            Whether to log progress of the algorithm execution
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads used for the algorithm execution.
        job_id : Optional[Any], default=None
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
        max_iterations: Optional[int] = None,
        tolerance: Optional[float] = None,
        source_nodes: Optional[Any] = None,
        scaler: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
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
        max_iterations : Optional[int], default=None
            The maximum number of iterations to run the algorithm
        tolerance : Optional[float], default=None
            The tolerance for convergence detection
        source_nodes : Optional[Any], default=None
            The source nodes to start the computation from
        scaler : Optional[Any], default=None
            Scaling configuration for the algorithm
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight values for relationships
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : Optional[bool], default=None
            Whether to log progress of the algorithm execution
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads used for the algorithm execution.
        job_id : Optional[Any], default=None
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
        max_iterations: Optional[int] = None,
        tolerance: Optional[float] = None,
        source_nodes: Optional[Any] = None,
        scaler: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> DataFrame:
        """
        Executes the Eigenvector Centrality algorithm and returns a stream of results.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        max_iterations : Optional[int], default=None
            The maximum number of iterations to run the algorithm
        tolerance : Optional[float], default=None
            The tolerance for convergence detection
        source_nodes : Optional[Any], default=None
            The source nodes to start the computation from
        scaler : Optional[Any], default=None
            Scaling configuration for the algorithm
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight values for relationships
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : Optional[bool], default=None
            Whether to log progress of the algorithm execution
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads used for the algorithm execution.
        job_id : Optional[Any], default=None
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
        max_iterations: Optional[int] = None,
        tolerance: Optional[float] = None,
        source_nodes: Optional[Any] = None,
        scaler: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
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
        max_iterations : Optional[int], default=None
            The maximum number of iterations to run the algorithm
        tolerance : Optional[float], default=None
            The tolerance for convergence detection
        source_nodes : Optional[Any], default=None
            The source nodes to start the computation from
        scaler : Optional[Any], default=None
            Scaling configuration for the algorithm
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight values for relationships
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : Optional[bool], default=None
            Whether to log progress of the algorithm execution
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads used for the algorithm execution.
        job_id : Optional[Any], default=None
            An identifier for the job that can be used for monitoring and cancellation
        write_concurrency : Optional[Any], default=None
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
        G: Union[GraphV2, dict[str, Any]],
        max_iterations: Optional[int] = None,
        tolerance: Optional[float] = None,
        source_nodes: Optional[Any] = None,
        scaler: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : Union[GraphV2, dict[str, Any]]
            The graph to run the algorithm on or a dictionary representing the graph configuration.
        max_iterations : Optional[int], default=None
            The maximum number of iterations to run the algorithm
        tolerance : Optional[float], default=None
            The tolerance for convergence detection
        source_nodes : Optional[Any], default=None
            The source nodes to start the computation from
        scaler : Optional[Any], default=None
            Scaling configuration for the algorithm
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight values for relationships
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run.
        concurrency : Optional[Any], default=None
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
