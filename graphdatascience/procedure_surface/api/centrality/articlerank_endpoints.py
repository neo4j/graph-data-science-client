from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class ArticleRankEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        damping_factor: float | None = None,
        tolerance: float | None = None,
        max_iterations: int | None = None,
        scaler: Any | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: Any | None = None,
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
        damping_factor : float | None, default=None
            Probability of a jump to a random node.
        tolerance : float | None, default=None
            Minimum change in scores between iterations.
        max_iterations : int | None, default=None
            Maximum number of iterations to run.
        scaler : Any | None, default=None
            Name of the scaler applied on the resulting scores.
        relationship_types : list[str] | None, default=None
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels : list[str] | None, default=None
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : bool | None, default=None
            Disable the memory guard.
        log_progress : bool | None, default=None
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            Number of threads to use for running the algorithm.
        job_id : Any | None, default=None
            Identifier for the job.
        relationship_weight_property : str | None, default=None
            Name of the property to be used as weights.
        source_nodes : Any | None, default=None
            List of node ids to use as starting points. Use a list of list pairs to associate each node with a bias > 0.

        Returns
        -------
        ArticleRankMutateResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        damping_factor: float | None = None,
        tolerance: float | None = None,
        max_iterations: int | None = None,
        scaler: Any | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: Any | None = None,
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
        damping_factor : float | None, default=None
            Probability of a jump to a random node.
        tolerance : float | None, default=None
            Minimum change in scores between iterations.
        max_iterations : int | None, default=None
            Maximum number of iterations to run.
        scaler : Any | None, default=None
            Name of the scaler applied on the resulting scores.
        relationship_types : list[str] | None, default=None
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels : list[str] | None, default=None
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : bool | None, default=None
            Disable the memory guard.
        log_progress : bool | None, default=None
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            Number of threads to use for running the algorithm.
        job_id : Any | None, default=None
            Identifier for the job.
        relationship_weight_property : str | None, default=None
            Name of the property to be used as weights.
        source_nodes : Any | None, default=None
            List of node ids to use as starting points. Use a list of list pairs to associate each node with a bias > 0.

        Returns
        -------
        ArticleRankStatsResult
            Algorithm statistics
        """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        damping_factor: float | None = None,
        tolerance: float | None = None,
        max_iterations: int | None = None,
        scaler: Any | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: Any | None = None,
    ) -> DataFrame:
        """
        Executes the ArticleRank algorithm and returns the results as a stream.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        damping_factor : float | None, default=None
            The damping factor controls the probability of a random jump to a random node
        tolerance : float | None, default=None
            Minimum change in scores between iterations
        max_iterations : int | None, default=None
            The maximum number of iterations to run
        scaler : Any | None, default=None
            Configuration for scaling the scores
        relationship_types : list[str] | None, default=None
            The relationships types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        sudo : bool | None, default=None
            Override memory estimation limits
        log_progress : bool | None, default=None
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            The number of concurrent threads
        job_id : Any | None, default=None
            An identifier for the job
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        source_nodes : Any | None, default=None
            The source nodes for personalized ArticleRank

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
        damping_factor: float | None = None,
        tolerance: float | None = None,
        max_iterations: int | None = None,
        scaler: Any | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: Any | None = None,
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
        damping_factor : float | None, default=None
            Probability of a jump to a random node.
        tolerance : float | None, default=None
            Minimum change in scores between iterations.
        max_iterations : int | None, default=None
            Maximum number of iterations to run.
        scaler : Any | None, default=None
            Name of the scaler applied on the resulting scores.
        relationship_types : list[str] | None, default=None
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels : list[str] | None, default=None
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : bool | None, default=None
            Disable the memory guard.
        log_progress : bool | None, default=None
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            Number of threads to use for running the algorithm.
        job_id : Any | None, default=None
            Identifier for the job.
        relationship_weight_property : str | None, default=None
            Name of the property to be used as weights.
        source_nodes : Any | None, default=None
            List of node ids to use as starting points. Use a list of list pairs to associate each node with a bias > 0.
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
        damping_factor: float | None = None,
        tolerance: float | None = None,
        max_iterations: int | None = None,
        scaler: Any | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: Any | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on or a dictionary representing the graph.
        damping_factor : float | None, default=None
            The damping factor controls the probability of a random jump to a random node
        tolerance : float | None, default=None
            Minimum change in scores between iterations
        max_iterations : int | None, default=None
            The maximum number of iterations to run
        scaler : Any | None, default=None
            Configuration for scaling the scores
        relationship_types : list[str] | None, default=None
            The relationships types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        concurrency : Any | None, default=None
            The number of concurrent threads
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        source_nodes : Any | None, default=None
            The source nodes for personalized ArticleRank

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
