from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class SllpaEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        max_iterations: int,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        min_association_strength: float | None = None,
        node_labels: list[str] | None = None,
        partitioning: Any | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
        username: str | None = None,
    ) -> SllpaMutateResult:
        """
        Executes the Speaker-Listener Label Propagation algorithm (SLLPA) and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the community ID for each node
        max_iterations : int
            Maximum number of iterations for the algorithm
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        min_association_strength : float | None, default=None
            Minimum association strength for community assignment
        node_labels : list[str] | None, default=None
            Filter nodes with specific labels
        partitioning : Any | None, default=None
            Partitioning configuration for the algorithm
        relationship_types : list[str] | None, default=None
            Filter relationships with specific types
        sudo : bool | None, default=False
            Run with elevated privileges
        username : str | None, default=None
            Username for authentication

        Returns
        -------
        SllpaMutateResult
            An object containing metadata about the algorithm execution and the mutation
        """
        ...

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        *,
        max_iterations: int,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        min_association_strength: float | None = None,
        node_labels: list[str] | None = None,
        partitioning: Any | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
        username: str | None = None,
    ) -> SllpaStatsResult:
        """
        Executes the Speaker-Listener Label Propagation algorithm (SLLPA) and returns statistics about the communities.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        min_association_strength : float | None, default=None
            Minimum association strength for community assignment
        node_labels : list[str] | None, default=None
            Filter nodes with specific labels
        partitioning : Any | None, default=None
            Partitioning configuration for the algorithm
        relationship_types : list[str] | None, default=None
            Filter relationships with specific types
        sudo : bool | None, default=False
            Run with elevated privileges
        username : str | None, default=None
            Username for authentication

        Returns
        -------
        SllpaStatsResult
            An object containing statistics about the algorithm execution
        """
        ...

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        *,
        max_iterations: int,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        min_association_strength: float | None = None,
        node_labels: list[str] | None = None,
        partitioning: Any | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
        username: str | None = None,
    ) -> DataFrame:
        """
        Executes the Speaker-Listener Label Propagation algorithm (SLLPA) and returns the results as a DataFrame.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        min_association_strength : float | None, default=None
            Minimum association strength for community assignment
        node_labels : list[str] | None, default=None
            Filter nodes with specific labels
        partitioning : Any | None, default=None
            Partitioning configuration for the algorithm
        relationship_types : list[str] | None, default=None
            Filter relationships with specific types
        sudo : bool | None, default=False
            Run with elevated privileges
        username : str | None, default=None
            Username for authentication

        Returns
        -------
        DataFrame
            DataFrame containing node IDs and their community values
        """
        ...

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        max_iterations: int,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        min_association_strength: float | None = None,
        node_labels: list[str] | None = None,
        partitioning: Any | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> SllpaWriteResult:
        """
        Executes the Speaker-Listener Label Propagation algorithm (SLLPA) and writes the results back to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to store the community ID for each node in the database
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        min_association_strength : float | None, default=None
            Minimum association strength for community assignment
        node_labels : list[str] | None, default=None
            Filter nodes with specific labels
        partitioning : Any | None, default=None
            Partitioning configuration for the algorithm
        relationship_types : list[str] | None, default=None
            Filter relationships with specific types
        sudo : bool | None, default=False
            Run with elevated privileges
        username : str | None, default=None
            Username for authentication
        write_concurrency : int | None, default=None
            The number of concurrent threads for writing

        Returns
        -------
        SllpaWriteResult
            An object containing metadata about the algorithm execution and the write operation
        """
        ...

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        max_iterations: int,
        concurrency: int | None = None,
        min_association_strength: float | None = None,
        node_labels: list[str] | None = None,
        partitioning: Any | None = None,
        relationship_types: list[str] | None = None,
    ) -> EstimationResult:
        """
        Estimates the memory consumption for running the Speaker-Listener Label Propagation algorithm (SLLPA).

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to estimate for, or a graph configuration dictionary
        concurrency : int | None, default=None
            The number of concurrent threads
        min_association_strength : float | None, default=None
            Minimum association strength for community assignment
        node_labels : list[str] | None, default=None
            Filter nodes with specific labels
        partitioning : Any | None, default=None
            Partitioning configuration for the algorithm
        relationship_types : list[str] | None, default=None
            Filter relationships with specific types

        Returns
        -------
        EstimationResult
            An object containing the memory estimation
        """
        ...


class SllpaMutateResult(BaseResult):
    """
    Represents the result of the Speaker-Listener Label Propagation algorithm (SLLPA) mutate operation.

    Attributes
    ----------
    ran_iterations : int
        The number of iterations the algorithm ran
    did_converge : bool
        Whether the algorithm converged
    pre_processing_millis : int
        Time spent preprocessing in milliseconds
    compute_millis : int
        Time spent computing in milliseconds
    mutate_millis : int
        Time spent writing results to the graph in milliseconds
    node_properties_written : int
        The number of node properties written
    configuration : dict
        The configuration used for the algorithm
    """

    ran_iterations: int
    did_converge: bool
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    node_properties_written: int
    configuration: dict[str, Any]


class SllpaStatsResult(BaseResult):
    """
    Represents the result of the Speaker-Listener Label Propagation algorithm (SLLPA) stats operation.

    Attributes
    ----------
    ran_iterations : int
        The number of iterations the algorithm ran
    did_converge : bool
        Whether the algorithm converged
    pre_processing_millis : int
        Time spent preprocessing in milliseconds
    compute_millis : int
        Time spent computing in milliseconds
    configuration : dict
        The configuration used for the algorithm
    """

    ran_iterations: int
    did_converge: bool
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class SllpaWriteResult(BaseResult):
    """
    Represents the result of the Speaker-Listener Label Propagation algorithm (SLLPA) write operation.

    Attributes
    ----------
    ran_iterations : int
        The number of iterations the algorithm ran
    did_converge : bool
        Whether the algorithm converged
    pre_processing_millis : int
        Time spent preprocessing in milliseconds
    compute_millis : int
        Time spent computing in milliseconds
    write_millis : int
        Time spent writing results to the database in milliseconds
    node_properties_written : int
        The number of node properties written
    configuration : dict
        The configuration used for the algorithm
    """

    ran_iterations: int
    did_converge: bool
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    node_properties_written: int
    configuration: dict[str, Any]
