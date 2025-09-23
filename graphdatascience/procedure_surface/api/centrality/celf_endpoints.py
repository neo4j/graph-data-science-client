from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class CelfEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        seed_set_size: int,
        mutate_property: str,
        propagation_probability: Optional[float] = None,
        monte_carlo_simulations: Optional[int] = None,
        random_seed: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> CelfMutateResult:
        """
        Runs the CELF algorithm and stores the results in the graph catalog as a new node property.

        The influence maximization problem asks for a set of k nodes that maximize the expected spread of influence in the network.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        seed_set_size : int
            The number of nodes to select as the seed set for influence maximization
        mutate_property : str
            Name of the node property to store the results in.
        propagation_probability : Optional[float], default=None
            Probability of a node being activated by an active neighbour node.
        monte_carlo_simulations : Optional[int], default=None
            Number of Monte-Carlo simulations.
        random_seed : Optional[Any], default=None
            Random seed for reproducible results.
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

        Returns
        -------
        CelfMutateResult
            Algorithm metrics and statistics including the total influence spread
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        seed_set_size: int,
        propagation_probability: Optional[float] = None,
        monte_carlo_simulations: Optional[int] = None,
        random_seed: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> CelfStatsResult:
        """
        Runs the CELF algorithm and returns result statistics without storing the results.

        The influence maximization problem asks for a set of k nodes that maximize the expected spread of influence in the network.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        seed_set_size : int
            The number of nodes to select as the seed set for influence maximization
        propagation_probability : Optional[float], default=None
            Probability of a node being activated by an active neighbour node.
        monte_carlo_simulations : Optional[int], default=None
            Number of Monte-Carlo simulations.
        random_seed : Optional[Any], default=None
            Random seed for reproducible results.
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

        Returns
        -------
        CelfStatsResult
            Algorithm statistics including the total influence spread
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        seed_set_size: int,
        propagation_probability: Optional[float] = None,
        monte_carlo_simulations: Optional[int] = None,
        random_seed: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> DataFrame:
        """
        Executes the CELF algorithm and returns a stream of results.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        seed_set_size : int
            The number of nodes to select as the seed set for influence maximization
        propagation_probability : Optional[float], default=None
            The probability that influence spreads from one node to another.
        monte_carlo_simulations : Optional[int], default=None
            The number of Monte-Carlo simulations.
        random_seed : Optional[Any], default=None
            Random seed for reproducible results.
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
            DataFrame with nodeId and spread columns containing CELF results.
            Each row represents a selected node with its corresponding influence spread value.
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        seed_set_size: int,
        write_property: str,
        propagation_probability: Optional[float] = None,
        monte_carlo_simulations: Optional[int] = None,
        random_seed: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
    ) -> CelfWriteResult:
        """
        Runs the CELF algorithm and stores the result in the Neo4j database as a new node property.

        The influence maximization problem asks for a set of k nodes that maximize the expected spread of influence in the network.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        seed_set_size : int
            The number of nodes to select as the seed set for influence maximization
        write_property : str
            Name of the node property to store the results in.
        propagation_probability : Optional[float], default=None
            Probability of a node being activated by an active neighbour node.
        monte_carlo_simulations : Optional[int], default=None
            Number of Monte-Carlo simulations.
        random_seed : Optional[Any], default=None
            Random seed for reproducible results.
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
        write_concurrency : Optional[Any], default=None
            The number of concurrent threads used during the write phase.

        Returns
        -------
        CelfWriteResult
            Algorithm metrics and statistics including the total influence spread and write timing
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        seed_set_size: int,
        propagation_probability: Optional[float] = None,
        monte_carlo_simulations: Optional[int] = None,
        random_seed: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : Union[GraphV2, dict[str, Any]]
            The graph to be used in the estimation. Provided either as a GraphV2 object or a configuration dictionary for the projection.
        seed_set_size : int
            The number of nodes to select as the seed set for influence maximization.
        propagation_probability : Optional[float], default=None
            The probability that influence spreads from one node to another.
        monte_carlo_simulations : Optional[int], default=None
            The number of Monte-Carlo simulations.
        random_seed : Optional[Any], default=None
            Random seed for reproducible results.
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run.
        concurrency : Optional[Any], default=None
            The number of concurrent threads used for the estimation.

        Returns
        -------
        EstimationResult
            An object containing the result of the estimation including memory requirements
        """
        pass


class CelfMutateResult(BaseResult):
    """Result of running CELF algorithm with mutate mode."""

    node_properties_written: int
    mutate_millis: int
    compute_millis: int
    total_spread: float
    node_count: int
    configuration: dict[str, Any]


class CelfStatsResult(BaseResult):
    """Result of running CELF algorithm with stats mode."""

    compute_millis: int
    total_spread: float
    node_count: int
    configuration: dict[str, Any]


class CelfWriteResult(BaseResult):
    """Result of running CELF algorithm with write mode."""

    node_properties_written: int
    write_millis: int
    compute_millis: int
    total_spread: float
    node_count: int
    configuration: dict[str, Any]
