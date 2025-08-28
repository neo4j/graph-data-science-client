from __future__ import annotations

from abc import ABC, abstractmethod
from types import TracebackType
from typing import List, NamedTuple, Optional, Type

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2


class GraphSamplingEndpoints(ABC):
    """
    Abstract base class defining the API for graph sampling operations.
    """

    @abstractmethod
    def rwr(
        self,
        G: GraphV2,
        graph_name: str,
        start_nodes: Optional[List[int]] = None,
        restart_probability: Optional[float] = None,
        sampling_ratio: Optional[float] = None,
        node_label_stratification: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
    ) -> GraphWithSamplingResult:
        """
        Random walk with restarts (RWR) samples the graph by taking random walks from a set of start nodes.

	    On each step of a random walk, there is a probability that the walk stops, and a new walk from one of the start
	    nodes starts instead (i.e. the walk restarts). Each node visited on these walks will be part of the sampled
	    subgraph. The resulting subgraph is stored as a new graph in the Graph Catalog.

        Parameters
        ----------
        G : GraphV2
            The input graph on which the Random Walk with Restart (RWR) will be
            performed.
        graph_name : str
            The name of the new graph that is stored in the graph catalog.
        start_nodes : list of int, optional
            IDs of the initial set of nodes in the original graph from which the sampling random walks will start.
	        By default, a single node is chosen uniformly at random.
        restart_probability : float, optional
            The probability that a sampling random walk restarts from one of the start nodes.
            Default is 0.1.
        sampling_ratio : float, optional
            The fraction of nodes in the original graph to be sampled.
            Default is 0.15.
        node_label_stratification : bool, optional
            If true, preserves the node label distribution of the original graph.
            Default is False.
        relationship_weight_property : str, optional
            Name of the relationship property to use as weights. If unspecified, the algorithm runs unweighted.
        relationship_types : list of str, optional
            Filter the named graph using the given relationship types. Relationships with any of the given types will be
            included.
        node_labels : list of str, optional
            Filter the named graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : bool, optional
            Bypass heap control. Use with caution.
            Default is False.
        log_progress : bool, optional
            Turn `on/off` percentage logging while running procedure.
            Default is True.
        username : str, optional
            Use Administrator access to run an algorithm on a graph owned by another user.
            Default is None.
        concurrency : int, optional
            The number of concurrent threads used for running the algorithm.
            Default is 4.
        job_id : str, optional
            An ID that can be provided to more easily track the algorithm’s progress.
            By default, a random job id is generated.

        Returns
        -------
        GraphWithSamplingResult
            Tuple of the graph object and the result of the Random Walk with Restart (RWR), including the dimensions of the sampled graph.
        """
        pass

    @abstractmethod
    def cnarw(
        self,
        G: GraphV2,
        graph_name: str,
        start_nodes: Optional[List[int]] = None,
        restart_probability: Optional[float] = None,
        sampling_ratio: Optional[float] = None,
        node_label_stratification: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
    ) -> GraphWithSamplingResult:
        """
        Computes a set of Random Walks with Restart (RWR) for the given graph and stores the result as a new graph in the catalog.

        This method performs a random walk, beginning from a set of nodes (if provided),
        where at each step there is a probability to restart back at the original nodes.
        The result is turned into a new graph induced by the random walks and stored in the catalog.

        Parameters
        ----------
        G : GraphV2
            The input graph on which the Random Walk with Restart (RWR) will be
            performed.
        graph_name : str
            The name of the new graph in the catalog.
        start_nodes : list of int, optional
            A list of node IDs to start the random walk from. If not provided, all
            nodes are used as potential starting points.
        restart_probability : float, optional
            The probability of restarting back to the original node at each step.
            Should be a value between 0 and 1. If not specified, a default value is used.
        sampling_ratio : float, optional
            The ratio of nodes to sample during the computation. This value should
            be between 0 and 1. If not specified, no sampling is performed.
        node_label_stratification : bool, optional
            If True, the algorithm tries to preserve the label distribution of the original graph in the sampled graph.
        relationship_weight_property : str, optional
            The name of the property on relationships to use as weights during
            the random walk. If not specified, the relationships are treated as
            unweighted.
        relationship_types : list of str, optional
            The relationship types used to select relationships for this algorithm run.
        node_labels : list of str, optional
            The node labels used to select nodes for this algorithm run.
        sudo : bool, optional
             Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : bool, optional
            If True, logs the progress of the computation.
        username : str, optional
            The username to attribute the procedure run to
        concurrency : int, optional
            The number of concurrent threads used for the algorithm execution.
        job_id : str, optional
            An identifier for the job that can be used for monitoring and cancellation

        Returns
        -------
        GraphSamplingResult
            Tuple of the graph object and the result of the Random Walk with Restart (RWR), including the sampled
            nodes and their scores.
        """
        pass


class GraphSamplingResult(BaseResult):
    graph_name: str
    from_graph_name: str
    node_count: int
    relationship_count: int
    start_node_count: int
    project_millis: int


class GraphWithSamplingResult(NamedTuple):
    graph: GraphV2
    result: GraphSamplingResult

    def __enter__(self) -> GraphV2:
        return self.graph

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.graph.drop()
