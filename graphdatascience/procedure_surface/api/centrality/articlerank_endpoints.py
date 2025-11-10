from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.scaler_config import ScalerConfig
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class ArticleRankEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: int | list[int] | list[tuple[int, float]] | None = None,
    ) -> ArticleRankMutateResult:
        """
        Runs the Article Rank algorithm and stores the results in the graph catalog as a new node property.

        ArticleRank is a variant of the Page Rank algorithm, which measures the transitive influence of nodes.
        Page Rank follows the assumption that relationships originating from low-degree nodes have a higher influence than relationships from high-degree nodes.
        Article Rank lowers the influence of low-degree nodes by lowering the scores being sent to their neighbors in each iteration.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            Name of the node property to store the results in.
        damping_factor : float
            Probability of a jump to a random node.
        tolerance : float
            Minimum change in scores between iterations.
        max_iterations : int
            Maximum number of iterations to run.
        scaler : str | dict[str, str | int | float] | ScalerConfig, default="NONE"
            The scaler to use. Can be:
            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center', 'NONE')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - A ScalerConfig instance
            - "NONE" (default, no scaling)
        relationship_types : list[str]
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels : list[str]
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : bool
            Disable the memory guard.
        log_progress : bool | None, default=None
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            Number of threads to use for running the algorithm.
        job_id : str | None
            Identifier for the job.
        relationship_weight_property : str | None, default=None
            Name of the property to be used as weights.
        source_nodes : int | list[int] | list[tuple[int, float]] | None, default=None
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
            - list of tuples to associate each node with a bias > 0 (e.g., [(42, 0.5), (43, 1.0)])

        Returns
        -------
        ArticleRankMutateResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        *,
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: int | list[int] | list[tuple[int, float]] | None = None,
    ) -> ArticleRankStatsResult:
        """
        Runs the Article Rank algorithm and returns result statistics without storing the results.

        ArticleRank is a variant of the Page Rank algorithm, which measures the transitive influence of nodes.
        Page Rank follows the assumption that relationships originating from low-degree nodes have a higher influence than relationships from high-degree nodes.
        Article Rank lowers the influence of low-degree nodes by lowering the scores being sent to their neighbors in each iteration.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        damping_factor : float
            Probability of a jump to a random node.
        tolerance : float
            Minimum change in scores between iterations.
        max_iterations : int
            Maximum number of iterations to run.
        scaler : str | dict[str, str | int | float] | ScalerConfig, default="NONE"
            The scaler to use. Can be:
            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center', 'NONE')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - A ScalerConfig instance
            - "NONE" (default, no scaling)
        relationship_types : list[str]
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels : list[str]
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : bool
            Disable the memory guard.
        log_progress : bool | None, default=None
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            Number of threads to use for running the algorithm.
        job_id : str | None
            Identifier for the job.
        relationship_weight_property : str | None, default=None
            Name of the property to be used as weights.
        source_nodes : int | list[int] | list[tuple[int, float]] | None, default=None
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
            - list of tuples to associate each node with a bias > 0 (e.g., [(42, 0.5), (43, 1.0)])

        Returns
        -------
        ArticleRankStatsResult
            Algorithm statistics
        """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        *,
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: int | list[int] | list[tuple[int, float]] | None = None,
    ) -> DataFrame:
        """
        Executes the ArticleRank algorithm and returns the results as a stream.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        damping_factor : float
            The damping factor controls the probability of a random jump to a random node
        tolerance : float
            Minimum change in scores between iterations
        max_iterations : int
            The maximum number of iterations to run
        scaler : Any
            Configuration for scaling the scores
        relationship_types : list[str]
            The relationships types used to select relationships for this algorithm run
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        sudo : bool
            Override memory estimation limits
        log_progress : bool
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            The number of concurrent threads
        job_id : str | None
            An identifier for the job
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        source_nodes : int | list[int] | list[tuple[int, float]] | None, default=None
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
            - list of tuples to associate each node with a bias > 0 (e.g., [(42, 0.5), (43, 1.0)])

        Returns
        -------
        DataFrame
            DataFrame with node IDs and their ArticleRank scores
        """

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: int | list[int] | list[tuple[int, float]] | None = None,
        write_concurrency: int | None = None,
    ) -> ArticleRankWriteResult:
        """
        Runs the Article Rank algorithm and stores the result in the Neo4j database as a new node property.

        ArticleRank is a variant of the Page Rank algorithm, which measures the transitive influence of nodes.
        Page Rank follows the assumption that relationships originating from low-degree nodes have a higher influence than relationships from high-degree nodes.
        Article Rank lowers the influence of low-degree nodes by lowering the scores being sent to their neighbors in each iteration.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to write the ArticleRank score for each node
        damping_factor : float
            Probability of a jump to a random node.
        tolerance : float
            Minimum change in scores between iterations.
        max_iterations : int
            Maximum number of iterations to run.
        scaler : str | dict[str, str | int | float] | ScalerConfig, default="NONE"
            The scaler to use. Can be:
            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center', 'NONE')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - A ScalerConfig instance
            - "NONE" (default, no scaling)
        relationship_types : list[str]
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels : list[str]
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : bool
            Disable the memory guard.
        log_progress : bool | None, default=None
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            Number of threads to use for running the algorithm.
        job_id : str | None
            Identifier for the job.
        relationship_weight_property : str | None, default=None
            Name of the property to be used as weights.
        source_nodes : int | list[int] | list[tuple[int, float]] | None, default=None
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
            - list of tuples to associate each node with a bias > 0 (e.g., [(42, 0.5), (43, 1.0)])
        write_concurrency : int | None, default=None
            The number of concurrent threads used for writing

        Returns
        -------
        ArticleRankWriteResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: int | list[int] | list[tuple[int, float]] | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on or a dictionary representing the graph.
        damping_factor : float
            The damping factor controls the probability of a random jump to a random node
        tolerance : float
            Minimum change in scores between iterations
        max_iterations : int
            The maximum number of iterations to run
        scaler : Any
            Configuration for scaling the scores
        relationship_types : list[str]
            The relationships types used to select relationships for this algorithm run
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        concurrency : int | None
            The number of concurrent threads
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        source_nodes : int | list[int] | list[tuple[int, float]] | None, default=None
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
            - list of tuples to associate each node with a bias > 0 (e.g., [(42, 0.5), (43, 1.0)])

        Returns
        -------
        EstimationResult
            Memory estimation details
        """


class ArticleRankMutateResult(BaseResult):
    ran_iterations: int
    did_converge: bool
    centrality_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    node_properties_written: int
    configuration: dict[str, Any]


class ArticleRankStatsResult(BaseResult):
    ran_iterations: int
    did_converge: bool
    centrality_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class ArticleRankWriteResult(BaseResult):
    ran_iterations: int
    did_converge: bool
    centrality_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    node_properties_written: int
    configuration: dict[str, Any]
