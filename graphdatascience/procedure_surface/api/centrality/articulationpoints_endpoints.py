from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2

from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class ArticulationPointsEndpoints(ABC):
    """
    Abstract base class defining the API for the Articulation Points algorithm.
    """

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> "ArticulationPointsMutateResult":
        """
        Executes the ArticulationPoints algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the articulation point flag for each node
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
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

        Returns
        -------
        ArticulationPointsMutateResult
            Algorithm metrics and statistics including the count of articulation points found
        """

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> "ArticulationPointsStatsResult":
        """
        Executes the ArticulationPoints algorithm and returns result statistics without writing the result to Neo4j.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
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

        Returns
        -------
        ArticulationPointsStatsResult
            Algorithm statistics including the count of articulation points found
        """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> "DataFrame":
        """
        Executes the ArticulationPoints algorithm and returns results as a stream.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
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

        Returns
        -------
        DataFrame
            A DataFrame containing articulation points with columns:
            - nodeId: The ID of the articulation point
            - resultingComponents: Information about resulting components
        """

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
    ) -> "ArticulationPointsWriteResult":
        """
        Executes the ArticulationPoints algorithm and writes the results back to the Neo4j database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to store the articulation point flag for each node
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
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
        write_concurrency : Optional[Any], default=None
            The number of concurrent threads for writing

        Returns
        -------
        ArticulationPointsWriteResult
            Algorithm metrics and statistics including the count of articulation points found
        """

    @abstractmethod
    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
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


class ArticulationPointsMutateResult(BaseResult):
    """Result of running ArticulationPoints algorithm with mutate mode."""

    articulation_point_count: int
    node_properties_written: int
    mutate_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class ArticulationPointsStatsResult(BaseResult):
    """Result of running ArticulationPoints algorithm with stats mode."""

    articulation_point_count: int
    compute_millis: int
    configuration: dict[str, Any]


class ArticulationPointsWriteResult(BaseResult):
    """Result of running ArticulationPoints algorithm with write mode."""

    articulation_point_count: int
    node_properties_written: int
    write_millis: int
    compute_millis: int
    configuration: dict[str, Any]
