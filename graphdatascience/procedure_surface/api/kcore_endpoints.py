from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional

from pandas import DataFrame
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from ...graph.graph_object import Graph
from .estimation_result import EstimationResult


class KCoreEndpoints(ABC):
    """
    Abstract base class defining the API for the K-Core decomposition algorithm.
    """

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        target_nodes: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> KCoreMutateResult:
        """
        Executes the K-Core algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the core value for each node
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
        target_nodes : Optional[Any], default=None
            Subset of nodes to compute the algorithm for
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight

        Returns
        -------
        KCoreMutateResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: Graph,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        target_nodes: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> KCoreStatsResult:
        """
        Executes the K-Core algorithm and returns statistics.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
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
        target_nodes : Optional[Any], default=None
            Subset of nodes to compute the algorithm for
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight

        Returns
        -------
        KCoreStatsResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: Graph,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        target_nodes: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DataFrame:
        """
        Executes the K-Core algorithm and returns a stream of results.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        relationship_types : Optional[List[str]], default=None
            The relationships types considered in this algorithm run
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
        target_nodes : Optional[Any], default=None
            Subset of nodes to compute the algorithm for
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight

        Returns
        -------
        DataFrame
            DataFrame with the algorithm results containing nodeId and coreValue
        """
        pass

    @abstractmethod
    def write(
        self,
        G: Graph,
        write_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        target_nodes: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[Any] = None,
    ) -> KCoreWriteResult:
        """
        Executes the K-Core algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        write_property : str
            The property name to write core values to
        relationship_types : Optional[List[str]], default=None
            The relationships types considered in this algorithm run
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
        target_nodes : Optional[Any], default=None
            Subset of nodes to compute the algorithm for
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        write_concurrency : Optional[Any], default=None
            The number of concurrent threads during the write phase

        Returns
        -------
        KCoreWriteResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: Optional[Graph] = None,
        projection_config: Optional[dict[str, Any]] = None,
    ) -> EstimationResult:
        """
        Estimate the results based on the provided graph and configuration.

        This abstract method is intended to be implemented in a subclass to provide
        specific functionality for estimating outcomes based on a graph name and a
        projection configuration object. The implementation should use the given
        parameters to compute and return an appropriate estimation result.

        Parameters
        ----------
        G : Optional[Graph], optional
            The graph to be used in the estimation
        projection_config : Optional[dict[str, Any]], optional
            Configuration dictionary for the projection.

        Returns
        -------
        EstimationResult
            An object containing the result of the estimation, with relevant details
            as defined by the specific implementation.
        """
        pass


class KCoreMutateResult(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    node_properties_written: int
    degeneracy: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    configuration: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)


class KCoreStatsResult(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    degeneracy: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)


class KCoreWriteResult(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    node_properties_written: int
    degeneracy: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    configuration: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)
