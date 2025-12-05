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
           Graph object to use
        mutate_property
            Name of the node property to store the results in.
        max_iterations
            Maximum number of iterations to run.
        tolerance
            Minimum change in scores between iterations.
        source_nodes : int | list[int] | list[tuple[int, float]] | None, default=None
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
        scaler
            The scaler to use. Can be:

            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center', 'NONE')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - - A :class:`~graphdatascience.procedure_surface.api.catalog.scaler_config.ScalerConfig` instance
        relationship_weight_property
            Name of the property to be used as weights.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

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
           Graph object to use
        max_iterations
            Maximum number of iterations to run.
        tolerance
            Minimum change in scores between iterations.
        source_nodes : int | list[int] | list[tuple[int, float]] | None, default=None
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
        scaler
            The scaler to use. Can be:

            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center', 'NONE')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - - A :class:`~graphdatascience.procedure_surface.api.catalog.scaler_config.ScalerConfig` instance
        relationship_weight_property
            Name of the property to be used as weights.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

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
           Graph object to use
        max_iterations
            Maximum number of iterations to run.
        tolerance
            Minimum change in scores between iterations.
        source_nodes : int | list[int] | list[tuple[int, float]] | None, default=None
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
        scaler
            The scaler to use. Can be:

            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center', 'NONE')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - - A :class:`~graphdatascience.procedure_surface.api.catalog.scaler_config.ScalerConfig` instance
        relationship_weight_property
            Name of the property to be used as weights.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

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
           Graph object to use
        write_property
            Name of the node property to store the results in.
        max_iterations
            Maximum number of iterations to run.
        tolerance
            Minimum change in scores between iterations.
        source_nodes : int | list[int] | list[tuple[int, float]] | None, default=None
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
        scaler
            The scaler to use. Can be:

            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center', 'NONE')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - - A :class:`~graphdatascience.procedure_surface.api.catalog.scaler_config.ScalerConfig` instance
        relationship_weight_property
            Name of the property to be used as weights.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        write_concurrency
            Number of concurrent threads to use for writing.Returns
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
        G
           Graph object to use or a dictionary representing the graph dimensions.
        max_iterations
            Maximum number of iterations to run.
        tolerance
            Minimum change in scores between iterations.
        source_nodes : int | list[int] | list[tuple[int, float]] | None, default=None
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
        scaler
            The scaler to use. Can be:

            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center', 'NONE')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - - A :class:`~graphdatascience.procedure_surface.api.catalog.scaler_config.ScalerConfig` instance
        relationship_weight_property
            Name of the property to be used as weights.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency
            Number of concurrent threads to use.

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
