from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class WccEndpoints(ABC):
    """
    Abstract base class defining the API for the Weakly Connected Components (WCC) algorithm.
    """

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        threshold: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool | None = None,
        relationship_weight_property: str | None = None,
    ) -> WccMutateResult:
        """
        Executes the WCC algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the component ID for each node
        threshold : float | None, default=None
            The minimum required weight to consider a relationship during traversal
        relationship_types : list[str] | None, default=None
            The relationships types used to select relationships for this algorithm run
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
        seed_property : str | None, default=None
            Defines node properties that are used as initial component identifiers
        consecutive_ids : bool | None, default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space
        relationship_weight_property : str | None, default=None
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
        G: GraphV2,
        threshold: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool | None = None,
        relationship_weight_property: str | None = None,
    ) -> WccStatsResult:
        """
        Executes the WCC algorithm and returns statistics.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        threshold : float | None, default=None
            The minimum required weight to consider a relationship during traversal
        relationship_types : list[str] | None, default=None
            The relationships types used to select relationships for this algorithm run
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
        seed_property : str | None, default=None
            Defines node properties that are used as initial component identifiers
        consecutive_ids : bool | None, default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space
        relationship_weight_property : str | None, default=None
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
        G: GraphV2,
        min_component_size: int | None = None,
        threshold: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool | None = None,
        relationship_weight_property: str | None = None,
    ) -> DataFrame:
        """
        Executes the WCC algorithm and returns a stream of results.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        min_component_size : int | None, default=None
            Don't stream components with fewer nodes than this
        threshold : float | None, default=None
            The minimum required weight to consider a relationship during traversal
        relationship_types : list[str] | None, default=None
            The relationships types considered in this algorithm run
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
        seed_property : str | None, default=None
            Defines node properties that are used as initial component identifiers
        consecutive_ids : bool | None, default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space
        relationship_weight_property : str | None, default=None
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
        G: GraphV2,
        write_property: str,
        min_component_size: int | None = None,
        threshold: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool | None = None,
        relationship_weight_property: str | None = None,
        write_concurrency: Any | None = None,
    ) -> WccWriteResult:
        """
        Executes the WCC algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to write component IDs to
        min_component_size : int | None, default=None
            Don't write components with fewer nodes than this
        threshold : float | None, default=None
            The minimum required weight to consider a relationship during traversal
        relationship_types : list[str] | None, default=None
            The relationships types considered in this algorithm run
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
        seed_property : str | None, default=None
            Defines node properties that are used as initial component identifiers
        consecutive_ids : bool | None, default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        write_concurrency : Any | None, default=None
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
        G: GraphV2 | dict[str, Any],
        threshold: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
        seed_property: str | None = None,
        consecutive_ids: bool | None = None,
        relationship_weight_property: str | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on or a dictionary representing the graph.
        threshold : float | None, default=None
            The minimum required weight to consider a relationship during traversal
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        concurrency : Any | None, default=None
            The number of concurrent threads
        seed_property : str | None, default=None
            A property to use as the starting component id for a node
        consecutive_ids : bool | None, default=None
            Flag to decide if the component identifiers should be returned consecutively or not
        relationship_weight_property : str | None, default=None
            The property name that contains weight

        Returns
        -------
        EstimationResult
            Memory estimation details
        """
        pass


class WccMutateResult(BaseResult):
    component_count: int
    component_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    node_properties_written: int
    configuration: dict[str, Any]


class WccStatsResult(BaseResult):
    component_count: int
    component_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class WccWriteResult(BaseResult):
    component_count: int
    component_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    post_processing_millis: int
    node_properties_written: int
    configuration: dict[str, Any]
