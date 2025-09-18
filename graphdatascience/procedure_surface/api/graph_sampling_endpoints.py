from __future__ import annotations

from abc import ABC, abstractmethod
from types import TracebackType
from typing import List, NamedTuple, Optional, Type

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2


class GraphSamplingEndpoints(ABC):
    """
    Abstract base class defining the API for graph sampling algorithms algorithm.
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
        GraphWithSamplingResult
            Tuple of the graph object and the result of the Random Walk with Restart (RWR), including the sampled
            nodes and their scores.
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
