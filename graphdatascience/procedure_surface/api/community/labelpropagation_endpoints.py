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
        consecutive_ids: bool | None = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int | None = 10,
        node_labels: list[str] = ALL_LABELS,
        node_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool | None = False,
        username: str | None = None,
    ) -> LabelPropagationMutateResult:
        """
        Executes the Label Propagation algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the community ID for each node
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool | None, default=False
            Whether to use consecutive community IDs starting from 0
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : int | None, default=10
            The maximum number of iterations
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        node_weight_property : str | None, default=None
            The property name for node weights
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : str | None, default=None
            The property name for relationship weights
        seed_property : str | None, default=None
            The property name containing seed values for initial community assignment
        sudo : bool | None, default=False
            Override memory estimation limits
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
        consecutive_ids: bool | None = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int | None = 10,
        node_labels: list[str] = ALL_LABELS,
        node_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool | None = False,
        username: str | None = None,
    ) -> LabelPropagationStatsResult:
        """
        Executes the Label Propagation algorithm and returns statistics.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool | None, default=False
            Whether to use consecutive community IDs starting from 0
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : int | None, default=10
            The maximum number of iterations
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        node_weight_property : str | None, default=None
            The property name for node weights
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : str | None, default=None
            The property name for relationship weights
        seed_property : str | None, default=None
            The property name containing seed values for initial community assignment
        sudo : bool | None, default=False
            Override memory estimation limits
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
        consecutive_ids: bool | None = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int | None = 10,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        node_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool | None = False,
        username: str | None = None,
    ) -> DataFrame:
        """
        Executes the Label Propagation algorithm and returns a stream of results.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool | None, default=False
            Whether to use consecutive community IDs starting from 0
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : int | None, default=10
            The maximum number of iterations
        min_community_size : int | None, default=None
            Minimum community size to include in results
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        node_weight_property : str | None, default=None
            The property name for node weights
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : str | None, default=None
            The property name for relationship weights
        seed_property : str | None, default=None
            The property name containing seed values for initial community assignment
        sudo : bool | None, default=False
            Override memory estimation limits
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
        consecutive_ids: bool | None = False,
        job_id: str | None = None,
        log_progress: bool = True,
        max_iterations: int | None = 10,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        node_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        seed_property: str | None = None,
        sudo: bool | None = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> LabelPropagationWriteResult:
        """
        Executes the Label Propagation algorithm and writes the results back to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to write the community IDs to
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool | None, default=False
            Whether to use consecutive community IDs starting from 0
        job_id : str | None, default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : int | None, default=10
            The maximum number of iterations
        min_community_size : int | None, default=None
            Minimum community size to include in results
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        node_weight_property : str | None, default=None
            The property name for node weights
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : str | None, default=None
            The property name for relationship weights
        seed_property : str | None, default=None
            The property name containing seed values for initial community assignment
        sudo : bool | None = False
            Override memory estimation limits
        username : str | None, default=None
            The username to attribute the procedure run to
        write_concurrency : int | None, default=None
            The number of concurrent threads for write operations

        Returns
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
        consecutive_ids: bool | None = False,
        max_iterations: int | None = 10,
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
        concurrency : int | None, default=None
            The number of concurrent threads
        consecutive_ids : bool | None, default=False
            Whether to use consecutive community IDs starting from 0
        max_iterations : int | None, default=10
            The maximum number of iterations
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        node_weight_property : str | None, default=None
            The property name for node weights
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : str | None, default=None
            The property name for relationship weights
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
    community_distribution: dict[str, Any]
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
    community_distribution: dict[str, Any]
    compute_millis: int
    configuration: dict[str, Any]
    did_converge: bool
    post_processing_millis: int
    pre_processing_millis: int
    ran_iterations: int


class LabelPropagationWriteResult(BaseResult):
    community_count: int
    community_distribution: dict[str, Any]
    compute_millis: int
    configuration: dict[str, Any]
    did_converge: bool
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int
    ran_iterations: int
    write_millis: int
