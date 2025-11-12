from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class LabelPropagationEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        node_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> LabelPropagationMutateResult:
        """
        Executes the Label Propagation algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        mutate_property
            Name of the node property to store the results in.
        concurrency
            Number of concurrent threads to use.
        consecutive_ids : bool, default=False
            Whether to use consecutive community IDs starting from 0
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_iterations : int, default=10
            The maximum number of iterations
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        node_weight_property : str | None, default=None
            The property name for node weights
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        seed_property : str | None, default=None
            The property name containing seed values for initial community assignment
        sudo
            Disable the memory guard.
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        LabelPropagationMutateResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        node_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> LabelPropagationStatsResult:
        """
        Executes the Label Propagation algorithm and returns statistics.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        concurrency
            Number of concurrent threads to use.
        consecutive_ids : bool, default=False
            Whether to use consecutive community IDs starting from 0
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_iterations : int, default=10
            The maximum number of iterations
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        node_weight_property : str | None, default=None
            The property name for node weights
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        seed_property : str | None, default=None
            The property name containing seed values for initial community assignment
        sudo
            Disable the memory guard.
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        LabelPropagationStatsResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int = 10,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        node_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        """
        Executes the Label Propagation algorithm and returns a stream of results.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        concurrency
            Number of concurrent threads to use.
        consecutive_ids : bool, default=False
            Whether to use consecutive community IDs starting from 0
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_iterations : int, default=10
            The maximum number of iterations
        min_community_size : int | None, default=None
            Minimum community size to include in results
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        node_weight_property : str | None, default=None
            The property name for node weights
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        seed_property : str | None, default=None
            The property name containing seed values for initial community assignment
        sudo
            Disable the memory guard.
        username : str | None, default=None
            The username to attribute the procedure run to

        Returns
        -------
        DataFrame
            DataFrame with the algorithm results containing nodeId and communityId
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int = 10,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        node_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> LabelPropagationWriteResult:
        """
        Executes the Label Propagation algorithm and writes the results back to the database.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_property
            Name of the node property to store the results in.
        concurrency
            Number of concurrent threads to use.
        consecutive_ids : bool, default=False
            Whether to use consecutive community IDs starting from 0
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        max_iterations : int, default=10
            The maximum number of iterations
        min_community_size : int | None, default=None
            Minimum community size to include in results
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        node_weight_property : str | None, default=None
            The property name for node weights
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        seed_property : str | None, default=None
            The property name containing seed values for initial community assignment
        sudo
            Disable the memory guard.
        username : str | None, default=None
            The username to attribute the procedure run to
        write_concurrency
            Number of concurrent threads to use for writing.Returns
        -------
        LabelPropagationWriteResult
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        concurrency: int | None = None,
        consecutive_ids: bool = False,
        max_iterations: int = 10,
        node_labels: list[str] = ALL_LABELS,
        node_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the Label Propagation algorithm.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph or graph configuration to estimate for
        concurrency
            Number of concurrent threads to use.
        consecutive_ids : bool, default=False
            Whether to use consecutive community IDs starting from 0
        max_iterations : int, default=10
            The maximum number of iterations
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        node_weight_property : str | None, default=None
            The property name for node weights
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_weight_property
            Name of the property to be used as weights.
        seed_property : str | None, default=None
            The property name containing seed values for initial community assignment

        Returns
        -------
        EstimationResult
            The memory estimation result
        """
        pass


class LabelPropagationMutateResult(BaseResult):
    community_count: int
    community_distribution: dict[str, int | float]
    compute_millis: int
    configuration: dict[str, Any]
    did_converge: bool
    mutate_millis: int
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int
    ran_iterations: int


class LabelPropagationStatsResult(BaseResult):
    community_count: int
    community_distribution: dict[str, int | float]
    compute_millis: int
    configuration: dict[str, Any]
    did_converge: bool
    post_processing_millis: int
    pre_processing_millis: int
    ran_iterations: int


class LabelPropagationWriteResult(BaseResult):
    community_count: int
    community_distribution: dict[str, int | float]
    compute_millis: int
    configuration: dict[str, Any]
    did_converge: bool
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int
    ran_iterations: int
    write_millis: int
