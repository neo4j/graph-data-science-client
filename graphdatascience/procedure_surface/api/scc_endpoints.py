from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from ...graph.graph_object import Graph
from .estimation_result import EstimationResult


class SccEndpoints(ABC):
    """
    Abstract base class defining the API for the Strongly Connected Components (SCC) algorithm.
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
        consecutive_ids: Optional[bool] = None,
    ) -> SccMutateResult:
        """
        Executes the SCC algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the component ID for each node
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
        consecutive_ids : Optional[bool], default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space

        Returns
        -------
        SccMutateResult
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
        consecutive_ids: Optional[bool] = None,
    ) -> SccStatsResult:
        """
        Executes the SCC algorithm and returns statistics.

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
        consecutive_ids : Optional[bool], default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space

        Returns
        -------
        SccStatsResult
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
        consecutive_ids: Optional[bool] = None,
    ) -> DataFrame:
        """
        Executes the SCC algorithm and returns a stream of results.

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
        consecutive_ids : Optional[bool], default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space

        Returns
        -------
        DataFrame
            DataFrame with the algorithm results
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
        consecutive_ids: Optional[bool] = None,
        write_concurrency: Optional[Any] = None,
    ) -> SccWriteResult:
        """
        Executes the SCC algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        write_property : str
            The property name to write component IDs to
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
        consecutive_ids : Optional[bool], default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space
        write_concurrency : Optional[Any], default=None
            The number of concurrent threads during the write phase

        Returns
        -------
        SccWriteResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def estimate(self, G: Union[Graph, dict[str, Any]]) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : Union[Graph, dict[str, Any]]
            The graph to run the algorithm on or a dictionary representing the graph.

        Returns
        -------
        EstimationResult
            An object containing the result of the estimation
        """
        pass


class SccMutateResult(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    component_count: int
    component_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    node_properties_written: int
    configuration: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)


class SccStatsResult(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    component_count: int
    component_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)


class SccWriteResult(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    component_count: int
    component_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    post_processing_millis: int
    node_properties_written: int
    configuration: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)
