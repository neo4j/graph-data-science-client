from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class DFSEndpoints(ABC):
    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        source_node: int,
        target_nodes: int | list[int] | None = None,
        max_depth: int = -1,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Runs the Depth First Search (DFS) algorithm and returns a stream of results.

        Parameters
        ----------
        G
           Graph object to use
        source_node
            Node id to use as the starting point.
        target_nodes : int | list[int], default=[]
            A single target node or a list of target nodes for the DFS computation.
        max_depth
            The maximum depth of the search.
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
        DataFrame
            DataFrame with sourceNode and nodeIds columns.
        """
        pass

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        source_node: int,
        target_nodes: int | list[int] = [],
        max_depth: int = -1,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DFSMutateResult:
        """
        Runs the Depth First Search (DFS) algorithm and stores the results as new relationships in the graph catalog.

        Parameters
        ----------
        G
           Graph object to use
        mutate_relationship_type
            Name of the relationship type to store the results in.
        source_node
            Node id to use as the starting point.
        target_nodes : int | list[int], default=[]
            A single target node or a list of target nodes for the DFS computation.
        max_depth
            The maximum depth of the search.
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
        DFSMutateResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        source_node: int,
        target_nodes: int | list[int] = [],
        max_depth: int = -1,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DFSStatsResult:
        """
        Runs the Depth First Search (DFS) algorithm and returns result statistics without storing the results.

        Parameters
        ----------
        G
           Graph object to use
        source_node
            Node id to use as the starting point.
        target_nodes : int | list[int], default=[]
            A single target node or a list of target nodes for the DFS computation.
        max_depth
            The maximum depth of the search.
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
        DFSStatsResult
            Algorithm statistics
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        source_node: int,
        target_nodes: int | list[int] = [],
        max_depth: int = -1,
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
        source_node
            Node id to use as the starting point.
        target_nodes : int | list[int], default=[]
            A single target node or a list of target nodes for the DFS computation.
        max_depth
            The maximum depth of the search.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency
            Number of concurrent threads to use.

        Returns
        -------
        EstimationResult
            Memory estimation details
        """
        pass


class DFSMutateResult(BaseResult):
    relationships_written: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    configuration: dict[str, Any]


class DFSStatsResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]
