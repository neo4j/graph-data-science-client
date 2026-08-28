from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class HitsEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: Graph,
        *,
        mutate_property: str = "",
        hits_iterations: int = 20,
        auth_property: str = "auth",
        hub_property: str = "hub",
        partitioning: str = "AUTO",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> HitsMutateResult:
        """
        Runs the HITS algorithm and stores the results in the graph catalog as new node properties.

        HITS (Hyperlink-Induced Topic Search) is a link analysis algorithm that computes two
        scores per node, a hub score and an authority score.

        Parameters
        ----------
        G
           Graph object to use
        mutate_property
            Postfix for the names of the node properties (auth and hub) to store the results in.
        hits_iterations
            Number of iterations to run HITS for.
        auth_property
            Name of the property holding the authority score.
        hub_property
            Name of the property holding the hub score.
        partitioning
            The partitioning scheme used to divide the work between threads.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

        Returns
        -------
        HitsMutateResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def stats(
        self,
        G: Graph,
        *,
        hits_iterations: int = 20,
        partitioning: str = "AUTO",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> HitsStatsResult:
        """
        Runs the HITS algorithm and returns result statistics without storing the results.

        Parameters
        ----------
        G
           Graph object to use
        hits_iterations
            Number of iterations to run HITS for.
        partitioning
            The partitioning scheme used to divide the work between threads.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

        Returns
        -------
        HitsStatsResult
            Algorithm statistics
        """

    @abstractmethod
    def stream(
        self,
        G: Graph,
        *,
        hits_iterations: int = 20,
        auth_property: str = "auth",
        hub_property: str = "hub",
        partitioning: str = "AUTO",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Executes the HITS algorithm and returns the results as a stream.

        Parameters
        ----------
        G
           Graph object to use
        hits_iterations
            Number of iterations to run HITS for.
        auth_property
            Name of the property holding the authority score.
        hub_property
            Name of the property holding the hub score.
        partitioning
            The partitioning scheme used to divide the work between threads.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

        Returns
        -------
        pandas.DataFrame
            DataFrame with node IDs and their HITS scores (hub and auth) in a ``values`` map column
        """

    @abstractmethod
    def write(
        self,
        G: Graph,
        *,
        write_property: str = "",
        hits_iterations: int = 20,
        auth_property: str = "auth",
        hub_property: str = "hub",
        partitioning: str = "AUTO",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> HitsWriteResult:
        """
        Runs the HITS algorithm and stores the results in the Neo4j database as new node properties.

        Parameters
        ----------
        G
           Graph object to use
        write_property
            Postfix for the names of the node properties (auth and hub) to store the results in.
        hits_iterations
            Number of iterations to run HITS for.
        auth_property
            Name of the property holding the authority score.
        hub_property
            Name of the property holding the hub score.
        partitioning
            The partitioning scheme used to divide the work between threads.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        write_concurrency
            Number of concurrent threads to use for writing.

        Returns
        -------
        HitsWriteResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def estimate(
        self,
        G: Graph | dict[str, Any],
        *,
        hits_iterations: int = 20,
        partitioning: str = "AUTO",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G
           Graph object to use or a dictionary representing the graph dimensions.
        hits_iterations
            Number of iterations to run HITS for.
        partitioning
            The partitioning scheme used to divide the work between threads.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency
            Number of concurrent threads to use.

        Returns
        -------
        EstimationResult
            An object containing the result of the estimation including memory requirements
        """


class HitsMutateResult(BaseResult):
    ran_iterations: int
    did_converge: bool
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    node_properties_written: int
    configuration: dict[str, Any]


class HitsStatsResult(BaseResult):
    ran_iterations: int
    did_converge: bool
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class HitsWriteResult(BaseResult):
    ran_iterations: int
    did_converge: bool
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    node_properties_written: int
    configuration: dict[str, Any]
