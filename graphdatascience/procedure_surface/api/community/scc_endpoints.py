from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class SccEndpoints(ABC):
    """
    Abstract base class defining the API for the Strongly Connected Components (SCC) algorithm.
    """

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        consecutive_ids: bool | None = None,
    ) -> SccMutateResult:
        """
        Executes the SCC algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the component ID for each node
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
        consecutive_ids : bool | None, default=None
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
        G: GraphV2,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        consecutive_ids: bool | None = None,
    ) -> SccStatsResult:
        """
        Executes the SCC algorithm and returns statistics.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
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
        consecutive_ids : bool | None, default=None
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
        G: GraphV2,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        consecutive_ids: bool | None = None,
    ) -> DataFrame:
        """
        Executes the SCC algorithm and returns a stream of results.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
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
        consecutive_ids : bool | None, default=None
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
        G: GraphV2,
        write_property: str,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        consecutive_ids: bool | None = None,
        write_concurrency: Any | None = None,
    ) -> SccWriteResult:
        """
        Executes the SCC algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to write component IDs to
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
        consecutive_ids : bool | None, default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space
        write_concurrency : Any | None, default=None
            The number of concurrent threads during the write phase

        Returns
        -------
        SccWriteResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
        consecutive_ids: bool | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on or a dictionary representing the graph.
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        concurrency : Any | None, default=None
            The number of concurrent threads
        consecutive_ids : bool | None, default=None
            Flag to decide if the component identifiers should be returned consecutively or not

        Returns
        -------
        EstimationResult
            Memory estimation details
        """
        pass


class SccMutateResult(BaseResult):
    component_count: int
    component_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    node_properties_written: int
    configuration: dict[str, Any]


class SccStatsResult(BaseResult):
    component_count: int
    component_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class SccWriteResult(BaseResult):
    component_count: int
    component_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    post_processing_millis: int
    node_properties_written: int
    configuration: dict[str, Any]
