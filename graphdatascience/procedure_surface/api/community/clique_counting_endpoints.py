from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class CliqueCountingEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> CliqueCountingMutateResult:
        """
        Executes the clique counting algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        mutate_property : str
            The property name to store the clique counts for each node
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        sudo : bool, default=False
            Override memory estimation limits
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        CliqueCountingMutateResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> CliqueCountingStatsResult:
        """
        Executes the clique counting algorithm and returns statistics.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        sudo : bool | None, default=False
            Override memory estimation limits
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        CliqueCountingStatsResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool | None = False,
        username: str | None = None,
    ) -> DataFrame:
        """
        Executes the clique counting algorithm and returns a stream of results.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        sudo : bool | None, default=False
            Override memory estimation limits
        username : str | None, default=None
            The username to attribute the procedure run to

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
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> CliqueCountingWriteResult:
        """
        Executes the clique counting algorithm and writes the results back to the database.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_property : str
            The property name to write the clique counts to
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        sudo : bool | None, default=False
            Override memory estimation limits
        username : str | None, default=None
            The username to attribute the procedure run to
        write_concurrency : int | None, default=None
            The number of concurrent threads for write operations

        Returns
        -------
        CliqueCountingWriteResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        concurrency: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the clique counting algorithm.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph or graph configuration to estimate for
        concurrency : int | None, default=None
            The number of concurrent threads
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run

        Returns
        -------
        EstimationResult
            The memory estimation result
        """
        pass


class CliqueCountingMutateResult(BaseResult):
    compute_millis: int
    configuration: dict[str, Any]
    global_count: list[int]
    mutate_millis: int
    node_properties_written: int
    pre_processing_millis: int


class CliqueCountingStatsResult(BaseResult):
    compute_millis: int
    configuration: dict[str, Any]
    global_count: list[int]
    pre_processing_millis: int


class CliqueCountingWriteResult(BaseResult):
    compute_millis: int
    configuration: dict[str, Any]
    global_count: list[int]
    node_properties_written: int
    pre_processing_millis: int
    write_millis: int
