from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class ArticulationPointsEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> "ArticulationPointsMutateResult":
        """
        Runs the Articulation Points algorithm and stores the results in the graph catalog as a new node property.

        Given a graph, an articulation point is a node whose removal increases the number of connected components in the graph.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the articulation point flag for each node
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        sudo : bool | None, default=None
            Override memory estimation limits
        log_progress : bool | None, default=None
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            The number of concurrent threads
        job_id : Any | None, default=None
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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> "ArticulationPointsStatsResult":
        """
        Runs the Articulation Points algorithm and returns result statistics without storing the results.

        Given a graph, an articulation point is a node whose removal increases the number of connected components in the graph.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        sudo : bool | None, default=None
            Override memory estimation limits
        log_progress : bool | None, default=None
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            The number of concurrent threads
        job_id : Any | None, default=None
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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> "DataFrame":
        """
        Executes the ArticulationPoints algorithm and returns results as a stream.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        sudo : bool | None, default=None
            Override memory estimation limits
        log_progress : bool | None, default=None
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            The number of concurrent threads
        job_id : Any | None, default=None
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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        write_concurrency: Any | None = None,
    ) -> "ArticulationPointsWriteResult":
        """
        Runs the Articulation Points algorithm and stores the result in the Neo4j database as a new node property.

        Given a graph, an articulation point is a node whose removal increases the number of connected components in the graph.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to store the articulation point flag for each node
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        sudo : bool | None, default=None
            Override memory estimation limits
        log_progress : bool | None, default=None
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            The number of concurrent threads
        job_id : Any | None, default=None
            An identifier for the job
        write_concurrency : Any | None, default=None
            The number of concurrent threads for writing

        Returns
        -------
        ArticulationPointsWriteResult
            Algorithm metrics and statistics including the count of articulation points found
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: Any | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to be used in the estimation. Provided either as a GraphV2 object or a configuration dictionary for the projection.
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run.
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run.
        concurrency : Any | None, default=None
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
