from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class CliqueCountingEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        concurrency: Optional[int] = 4,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> CliqueCountingMutateResult:
        """
        Executes the clique counting algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the clique counts for each node
        concurrency : Optional[int], default=4
            The number of concurrent threads
        job_id : Optional[str], default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        sudo : Optional[bool], default=False
            Override memory estimation limits
        username : Optional[str], default=None
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
        concurrency: Optional[int] = 4,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> CliqueCountingStatsResult:
        """
        Executes the clique counting algorithm and returns statistics.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        concurrency : Optional[int], default=4
            The number of concurrent threads
        job_id : Optional[str], default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        sudo : Optional[bool], default=False
            Override memory estimation limits
        username : Optional[str], default=None
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
        concurrency: Optional[int] = 4,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> DataFrame:
        """
        Executes the clique counting algorithm and returns a stream of results.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        concurrency : Optional[int], default=4
            The number of concurrent threads
        job_id : Optional[str], default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        sudo : Optional[bool], default=False
            Override memory estimation limits
        username : Optional[str], default=None
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
        concurrency: Optional[int] = 4,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
        write_concurrency: Optional[int] = None,
    ) -> CliqueCountingWriteResult:
        """
        Executes the clique counting algorithm and writes the results back to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to write the clique counts to
        concurrency : Optional[int], default=4
            The number of concurrent threads
        job_id : Optional[str], default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        sudo : Optional[bool], default=False
            Override memory estimation limits
        username : Optional[str], default=None
            The username to attribute the procedure run to
        write_concurrency : Optional[int], default=None
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
        G: Union[GraphV2, dict[str, Any]],
        concurrency: Optional[int] = 4,
        node_labels: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the clique counting algorithm.

        Parameters
        ----------
        G : Union[GraphV2, dict[str, Any]]
            The graph or graph configuration to estimate for
        concurrency : Optional[int], default=4
            The number of concurrent threads
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        relationship_types : Optional[List[str]], default=None
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
    global_count: List[int]
    mutate_millis: int
    node_properties_written: int
    pre_processing_millis: int


class CliqueCountingStatsResult(BaseResult):
    compute_millis: int
    configuration: dict[str, Any]
    global_count: List[int]
    pre_processing_millis: int


class CliqueCountingWriteResult(BaseResult):
    compute_millis: int
    configuration: dict[str, Any]
    global_count: List[int]
    node_properties_written: int
    pre_processing_millis: int
    write_millis: int
