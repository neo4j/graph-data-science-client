from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class MaxKCutEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        concurrency: Optional[int] = None,
        iterations: Optional[int] = None,
        job_id: Optional[str] = None,
        k: Optional[int] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
        vns_max_neighborhood_order: Optional[int] = None,
    ) -> MaxKCutMutateResult:
        """
        Executes the Approximate Maximum k-cut algorithm and writes the results to the in-memory graph as node properties.

        The Approximate Maximum k-cut algorithm is a community detection algorithm that partitions a graph into k communities
        such that the sum of weights of edges between different communities is maximized. It uses a
        variable neighborhood search (VNS) approach to find high-quality cuts.        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the community ID for each node
        concurrency : Optional[int], default=4
            The number of concurrent threads. Setting this to 1 will run the algorithm single-threaded.
        iterations : Optional[int], default=8
            The number of iterations the algorithm runs. More iterations may lead to better results but
            will increase computation time.
        job_id : Optional[str], default=None
            An identifier for the job that can be used to cancel or monitor progress
        k : Optional[int], default=2
            The number of communities to detect. Must be at least 2.
        log_progress : bool, default=True
            Whether to log progress information during execution
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run. If None, all nodes are used.
        random_seed : Optional[int], default=None
            Random seed for reproducible results. If None, a random seed is used.
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run. If None, all
            relationship types are used.
        relationship_weight_property : Optional[str], default=None
            The relationship weight property. If None, each relationship has weight 1.0.
        sudo : Optional[bool], default=False
            Override memory estimation limits. Setting this to True allows running the algorithm
            even if the estimated memory requirements exceed available memory.
        username : Optional[str], default=None
            The username to attribute the procedure run to
        vns_max_neighborhood_order : Optional[int], default=0
            The maximum neighborhood order for the Variable Neighborhood Search. Higher values may
            lead to better results but increase computation time.

        Returns
        -------
        MaxKCutMutateResult
            Algorithm metrics and statistics including the cut cost and processing times
        """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = None,
        iterations: Optional[int] = None,
        job_id: Optional[str] = None,
        k: Optional[int] = None,
        log_progress: bool = True,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
        vns_max_neighborhood_order: Optional[int] = None,
    ) -> DataFrame:
        """
        Executes the Approximate Maximum k-cut algorithm and returns a stream of results.

        The Approximate Maximum k-cut algorithm partitions a graph into k communities to maximize the cut cost.
        This method returns the community assignment for each node as a stream.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        concurrency : Optional[int], default=4
            The number of concurrent threads. Setting this to 1 will run the algorithm single-threaded.
        iterations : Optional[int], default=8
            The number of iterations the algorithm runs. More iterations may lead to better results but
            will increase computation time.
        job_id : Optional[str], default=None
            An identifier for the job that can be used to cancel or monitor progress
        k : Optional[int], default=2
            The number of communities to detect. Must be at least 2.
        log_progress : bool, default=True
            Whether to log progress information during execution
        min_community_size : Optional[int], default=None
            The minimum community size. Communities smaller than this will be filtered from results.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run. If None, all nodes are used.
        random_seed : Optional[int], default=None
            Random seed for reproducible results. If None, a random seed is used.
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run. If None, all
            relationship types are used.
        relationship_weight_property : Optional[str], default=None
            The relationship weight property. If None, each relationship has weight 1.0.
        sudo : Optional[bool], default=False
            Override memory estimation limits. Setting this to True allows running the algorithm
            even if the estimated memory requirements exceed available memory.
        username : Optional[str], default=None
            The username to attribute the procedure run to
        vns_max_neighborhood_order : Optional[int], default=0
            The maximum neighborhood order for the Variable Neighborhood Search. Higher values may
            lead to better results but increase computation time.

        Returns
        -------
        DataFrame
            A DataFrame with columns:
            - nodeId: The node identifier
            - communityId: The community assignment for the node
        """

    @abstractmethod
    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        *,
        concurrency: Optional[int] = None,
        iterations: Optional[int] = None,
        k: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        vns_max_neighborhood_order: Optional[int] = None,
    ) -> EstimationResult:
        """
        Estimate the memory requirements for running the Approximate Maximum k-cut algorithm.

        This method provides memory estimates without actually running the algorithm, helping you
        determine if you have sufficient memory available.

        Parameters
        ----------
        G : Union[GraphV2, dict[str, Any]]
            The graph to estimate for, or a graph configuration dictionary
        concurrency : Optional[int], default=4
            The number of concurrent threads. Setting this to 1 will run the algorithm single-threaded.
        iterations : Optional[int], default=8
            The number of iterations the algorithm runs
        k : Optional[int], default=2
            The number of communities to detect. Must be at least 2.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run. If None, all nodes are used.
        random_seed : Optional[int], default=None
            Random seed for reproducible results
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run. If None, all
            relationship types are used.
        relationship_weight_property : Optional[str], default=None
            The relationship weight property. If None, each relationship has weight 1.0.
        vns_max_neighborhood_order : Optional[int], default=0
            The maximum neighborhood order for the Variable Neighborhood Search

        Returns
        -------
        EstimationResult
            The memory estimation result including required memory in bytes and as heap percentage
        """


class MaxKCutMutateResult(BaseResult):
    """
    Result object returned by the Approximate Maximum k-cut mutate operation.

    Attributes
    ----------
    cut_cost : float
        The cost of the cut, representing the sum of weights of edges between different communities
    compute_millis : int
        Time spent on computation in milliseconds
    configuration : dict[str, Any]
        The configuration used for the algorithm execution
    mutate_millis : int
        Time spent on mutating the graph in milliseconds
    node_properties_written : int
        The number of node properties written to the graph
    post_processing_millis : int
        Time spent on post-processing in milliseconds
    pre_processing_millis : int
        Time spent on pre-processing in milliseconds
    """

    cut_cost: float
    compute_millis: int
    configuration: dict[str, Any]
    mutate_millis: int
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int
