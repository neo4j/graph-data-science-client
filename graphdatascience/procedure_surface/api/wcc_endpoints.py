from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional

from pandas import DataFrame
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from ...graph.graph_object import Graph
from .estimation_result import EstimationResult


class WccEndpoints(ABC):
    """
    Abstract base class defining the API for the Weakly Connected Components (WCC) algorithm.
    """

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        threshold: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> WccMutateResult:
        """
        Executes the WCC algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the component ID for each node
        threshold : Optional[float], default=None
            The minimum required weight to consider a relationship during traversal
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
        seed_property : Optional[str], default=None
            Defines node properties that are used as initial component identifiers
        consecutive_ids : Optional[bool], default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight

        Returns
        -------
        WccMutateResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: Graph,
        threshold: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> WccStatsResult:
        """
        Executes the WCC algorithm and returns statistics.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        threshold : Optional[float], default=None
            The minimum required weight to consider a relationship during traversal
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
        seed_property : Optional[str], default=None
            Defines node properties that are used as initial component identifiers
        consecutive_ids : Optional[bool], default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight

        Returns
        -------
        WccStatsResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: Graph,
        min_component_size: Optional[int] = None,
        threshold: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DataFrame:
        """
        Executes the WCC algorithm and returns a stream of results.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        min_component_size : Optional[int], default=None
            Don't stream components with fewer nodes than this
        threshold : Optional[float], default=None
            The minimum required weight to consider a relationship during traversal
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
        seed_property : Optional[str], default=None
            Defines node properties that are used as initial component identifiers
        consecutive_ids : Optional[bool], default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight

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
        min_component_size: Optional[int] = None,
        threshold: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[Any] = None,
    ) -> WccWriteResult:
        """
        Executes the WCC algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        write_property : str
            The property name to write component IDs to
        min_component_size : Optional[int], default=None
            Don't write components with fewer nodes than this
        threshold : Optional[float], default=None
            The minimum required weight to consider a relationship during traversal
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
        seed_property : Optional[str], default=None
            Defines node properties that are used as initial component identifiers
        consecutive_ids : Optional[bool], default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        write_concurrency : Optional[Any], default=None
            The number of concurrent threads during the write phase

        Returns
        -------
        WccWriteResult
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


class WccMutateResult(BaseModel):
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


class WccStatsResult(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    component_count: int
    component_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)


class WccWriteResult(BaseModel):
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
