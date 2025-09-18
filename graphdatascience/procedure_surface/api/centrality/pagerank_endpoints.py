from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2

from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class PageRankEndpoints(ABC):

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
    ) -> PageRankMutateResult:
        """
        Runs the PageRank algorithm and stores the results in the graph catalog as a new node property.

        The PageRank algorithm measures the importance of each node within the graph, based on the number of incoming relationships and the importance of the corresponding source nodes.
        The underlying assumption roughly speaking is that a page is only as important as the pages that link to it.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            Name of the node property to store the results in.
        damping_factor : Optional[float], default=None
            Probability of a jump to a random node.
        tolerance : Optional[float], default=None
            Minimum change in scores between iterations.
        max_iterations : Optional[int], default=None
            Maximum number of iterations to run.
        scaler : Optional[Any], default=None
            Name of the scaler applied on the resulting scores.
        relationship_types : Optional[List[str]], default=None
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels : Optional[List[str]], default=None
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : Optional[bool], default=None
            Disable the memory guard.
        log_progress : Optional[bool], default=None
            Display progress logging.
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            Number of threads to use for running the algorithm.
        job_id : Optional[Any], default=None
            Identifier for the job.
        relationship_weight_property : Optional[str], default=None
            Name of the property to be used as weights.
        source_nodes : Optional[Any], default=None
            List of node ids to use as starting points. Use a list of list pairs to associate each node with a bias > 0.

        Returns
        -------
        PageRankMutateResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
    ) -> PageRankStatsResult:
        """
        Runs the PageRank algorithm and returns result statistics without storing the results.

        The PageRank algorithm measures the importance of each node within the graph, based on the number of incoming relationships and the importance of the corresponding source nodes.
        The underlying assumption roughly speaking is that a page is only as important as the pages that link to it.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        damping_factor : Optional[float], default=None
            Probability of a jump to a random node.
        tolerance : Optional[float], default=None
            Minimum change in scores between iterations.
        max_iterations : Optional[int], default=None
            Maximum number of iterations to run.
        scaler : Optional[Any], default=None
            Name of the scaler applied on the resulting scores.
        relationship_types : Optional[List[str]], default=None
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels : Optional[List[str]], default=None
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : Optional[bool], default=None
            Disable the memory guard.
        log_progress : Optional[bool], default=None
            Display progress logging.
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            Number of threads to use for running the algorithm.
        job_id : Optional[Any], default=None
            Identifier for the job.
        relationship_weight_property : Optional[str], default=None
            Name of the property to be used as weights.
        source_nodes : Optional[Any], default=None
            List of node ids to use as starting points. Use a list of list pairs to associate each node with a bias > 0.

        Returns
        -------
        PageRankStatsResult
            Algorithm statistics
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
    ) -> DataFrame:
        """
        Executes the PageRank algorithm and returns a stream of results.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        damping_factor : Optional[float], default=None
            The damping factor controls the probability of a random jump to a random node
        tolerance : Optional[float], default=None
            Minimum change in scores between iterations
        max_iterations : Optional[int], default=None
            The maximum number of iterations to run
        scaler : Optional[Any], default=None
            Configuration for scaling the scores
        relationship_types : Optional[List[str]], default=None
            The relationships types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        source_nodes : Optional[Any], default=None
            The source nodes for personalized PageRank

        Returns
        -------
        DataFrame
            DataFrame with node IDs and their PageRank scores
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
        write_concurrency: Optional[int] = None,
    ) -> PageRankWriteResult:
        """
        Runs the PageRank algorithm and stores the result in the Neo4j database as a new node property.

        The PageRank algorithm measures the importance of each node within the graph, based on the number of incoming relationships and the importance of the corresponding source nodes.
        The underlying assumption roughly speaking is that a page is only as important as the pages that link to it.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            Name of the node property to store the results in.
        damping_factor : Optional[float], default=None
            Probability of a jump to a random node.
        tolerance : Optional[float], default=None
            Minimum change in scores between iterations.
        max_iterations : Optional[int], default=None
            Maximum number of iterations to run.
        scaler : Optional[Any], default=None
            Name of the scaler applied on the resulting scores.
        relationship_types : Optional[List[str]], default=None
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels : Optional[List[str]], default=None
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : Optional[bool], default=None
            Disable the memory guard.
        log_progress : Optional[bool], default=None
            Display progress logging.
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            Number of threads to use for running the algorithm.
        job_id : Optional[Any], default=None
            Identifier for the job.
        relationship_weight_property : Optional[str], default=None
            Name of the property to be used as weights.
        source_nodes : Optional[Any], default=None
            List of node ids to use as starting points. Use a list of list pairs to associate each node with a bias > 0.
        write_concurrency : Optional[int], default=None
            The number of concurrent threads used for writing

        Returns
        -------
        PageRankWriteResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        damping_factor: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_iterations: Optional[int] = None,
        scaler: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        source_nodes: Optional[Any] = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : Union[GraphV2, dict[str, Any]]
            The graph to run the algorithm on or a dictionary representing the graph.
        damping_factor : Optional[float], default=None
            The damping factor controls the probability of a random jump to a random node
        tolerance : Optional[float], default=None
            Minimum change in scores between iterations
        max_iterations : Optional[int], default=None
            The maximum number of iterations to run
        scaler : Optional[Any], default=None
            Configuration for scaling the scores
        relationship_types : Optional[List[str]], default=None
            The relationships types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        source_nodes : Optional[Any], default=None
            The source nodes for personalized PageRank

        Returns
        -------
        EstimationResult
            Memory estimation details
        """
        pass


class PageRankMutateResult(BaseResult):
    ran_iterations: int
    did_converge: bool
    centrality_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    node_properties_written: int
    configuration: dict[str, Any]


class PageRankStatsResult(BaseResult):
    ran_iterations: int
    did_converge: bool
    centrality_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class PageRankWriteResult(BaseResult):
    ran_iterations: int
    did_converge: bool
    centrality_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    node_properties_written: int
    configuration: dict[str, Any]
