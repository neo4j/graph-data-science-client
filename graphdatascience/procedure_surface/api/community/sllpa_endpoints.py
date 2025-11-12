from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
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
        min_association_strength: float = 0.2,
        node_labels: list[str] = ALL_LABELS,
        partitioning: str = "RANGE",
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> SllpaMutateResult:
        """
        Executes the Speaker-Listener Label Propagation algorithm (SLLPA) and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        mutate_property : str
            The property name to store the community ID for each node
        max_iterations : int
            Maximum number of iterations for the algorithm
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id
            Identifier for the computation.
        log_progress : bool, default=True
            Whether to log progress
        min_association_strength : float | None, default=None
            Minimum association strength for community assignment
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        partitioning : str | None
            Partitioning configuration for the algorithm
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
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
        min_association_strength: float = 0.2,
        node_labels: list[str] = ALL_LABELS,
        partitioning: str = "RANGE",
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> SllpaStatsResult:
        """
        Executes the Speaker-Listener Label Propagation algorithm (SLLPA) and returns statistics about the communities.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id
            Identifier for the computation.
        log_progress : bool, default=True
            Whether to log progress
        min_association_strength : float | None, default=None
            Minimum association strength for community assignment
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        partitioning : str | None
            Partitioning configuration for the algorithm
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
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
        min_association_strength: float = 0.2,
        node_labels: list[str] = ALL_LABELS,
        partitioning: str = "RANGE",
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        """
        Executes the Speaker-Listener Label Propagation algorithm (SLLPA) and returns the results as a DataFrame.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id
            Identifier for the computation.
        log_progress : bool, default=True
            Whether to log progress
        min_association_strength : float | None, default=None
            Minimum association strength for community assignment
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        partitioning : str | None
            Partitioning configuration for the algorithm
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
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
        min_association_strength: float = 0.2,
        node_labels: list[str] = ALL_LABELS,
        partitioning: str = "RANGE",
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> SllpaWriteResult:
        """
        Executes the Speaker-Listener Label Propagation algorithm (SLLPA) and writes the results back to the database.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_property : str
            The property name to store the community ID for each node in the database
        concurrency : int | None, default=None
            The number of concurrent threads
        job_id
            Identifier for the computation.
        log_progress : bool, default=True
            Whether to log progress
        min_association_strength : float | None, default=None
            Minimum association strength for community assignment
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        partitioning : str | None
            Partitioning configuration for the algorithm
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sudo
            Disable the memory guard.
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
        min_association_strength: float = 0.2,
        node_labels: list[str] = ALL_LABELS,
        partitioning: str = "RANGE",
        relationship_types: list[str] = ALL_TYPES,
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
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        partitioning : str | None
            Partitioning configuration for the algorithm
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.

        Returns
        -------
        EstimationResult
            An object containing the memory estimation
        """
        ...


class SllpaMutateResult(BaseResult):
    ran_iterations: int
    did_converge: bool
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    node_properties_written: int
    configuration: dict[str, Any]


class SllpaStatsResult(BaseResult):
    ran_iterations: int
    did_converge: bool
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class SllpaWriteResult(BaseResult):
    ran_iterations: int
    did_converge: bool
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    node_properties_written: int
    configuration: dict[str, Any]
