from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional

from graphdatascience import Graph
from graphdatascience.procedure_surface.api.base_result import BaseResult


class GraphSamplingEndpoints(ABC):
    """
    Abstract base class defining the API for graph sampling operations.
    """

    @abstractmethod
    def rwr(
        self,
        G: Graph,
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
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> GraphSamplingResult:
        """
        Random walk with restarts (RWR) samples the graph by taking random walks from a set of start nodes.

	    On each step of a random walk, there is a probability that the walk stops, and a new walk from one of the start
	    nodes starts instead (i.e. the walk restarts). Each node visited on these walks will be part of the sampled
	    subgraph. The resulting subgraph is stored as a new graph in the Graph Catalog.

        Parameters
        ----------
        G : Graph
            The input graph to be sampled.
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
        relationship_types : Optional[List[str]], default=None
            Filter the named graph using the given relationship types. Relationships with any of the given types will be
            included.
        node_labels : Optional[List[str]], default=None
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
        concurrency : Any, optional
            The number of concurrent threads used for running the algorithm.
            Default is 4.
        job_id : Any, optional
            An ID that can be provided to more easily track the algorithm’s progress.
            By default, a random job id is generated.

        Returns
        -------
        GraphSamplingResult
            The result of the Random Walk with Restart (RWR), including the dimensions of the sampled graph.
        """
        pass

    @abstractmethod
    def cnarw(
        self,
        G: Graph,
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
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> GraphSamplingResult:
        """
        Common Neighbour Aware Random Walk (CNARW) samples the graph by taking random walks from a set of start nodes

        CNARW is a graph sampling technique that involves optimizing the selection of the next-hop node. It takes into
        account the number of common neighbours between the current node and the next-hop candidates. On each step of a
        random walk, there is a probability that the walk stops, and a new walk from one of the start nodes starts
        instead (i.e. the walk restarts). Each node visited on these walks will be part of the sampled subgraph. The
        resulting subgraph is stored as a new graph in the Graph Catalog.

        Parameters
        ----------
        G : Graph
            The input graph to be sampled.
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
        relationship_types : Optional[List[str]], default=None
            Filter the named graph using the given relationship types. Relationships with any of the given types will be
            included.
        node_labels : Optional[List[str]], default=None
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
        concurrency : Any, optional
            The number of concurrent threads used for running the algorithm.
            Default is 4.
        job_id : Any, optional
            An ID that can be provided to more easily track the algorithm’s progress.
            By default, a random job id is generated.

        Returns
        -------
        GraphSamplingResult
            The result of the Common Neighbour Aware Random Walk (CNARW), including the dimensions of the sampled graph.
        """
        pass


class GraphSamplingResult(BaseResult):
    graph_name: str
    from_graph_name: str
    node_count: int
    relationship_count: int
    start_node_count: int
    project_millis: int
