from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class LabelPropagationEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        node_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> LabelPropagationMutateResult:
        """
        Executes the Label Propagation algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the community ID for each node
        concurrency : Optional[int], default=None
            The number of concurrent threads
        consecutive_ids : Optional[bool], default=False
            Whether to use consecutive community IDs starting from 0
        job_id : Optional[str], default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : Optional[int], default=10
            The maximum number of iterations
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        node_weight_property : Optional[str], default=None
            The property name for node weights
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : Optional[str], default=None
            The property name for relationship weights
        seed_property : Optional[str], default=None
            The property name containing seed values for initial community assignment
        sudo : Optional[bool], default=False
            Override memory estimation limits
        username : Optional[str], default=None
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
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        node_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> LabelPropagationStatsResult:
        """
        Executes the Label Propagation algorithm and returns statistics.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        concurrency : Optional[int], default=None
            The number of concurrent threads
        consecutive_ids : Optional[bool], default=False
            Whether to use consecutive community IDs starting from 0
        job_id : Optional[str], default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : Optional[int], default=10
            The maximum number of iterations
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        node_weight_property : Optional[str], default=None
            The property name for node weights
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : Optional[str], default=None
            The property name for relationship weights
        seed_property : Optional[str], default=None
            The property name containing seed values for initial community assignment
        sudo : Optional[bool], default=False
            Override memory estimation limits
        username : Optional[str], default=None
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
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        node_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> DataFrame:
        """
        Executes the Label Propagation algorithm and returns a stream of results.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        concurrency : Optional[int], default=None
            The number of concurrent threads
        consecutive_ids : Optional[bool], default=False
            Whether to use consecutive community IDs starting from 0
        job_id : Optional[str], default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : Optional[int], default=10
            The maximum number of iterations
        min_community_size : Optional[int], default=None
            Minimum community size to include in results
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        node_weight_property : Optional[str], default=None
            The property name for node weights
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : Optional[str], default=None
            The property name for relationship weights
        seed_property : Optional[str], default=None
            The property name containing seed values for initial community assignment
        sudo : Optional[bool], default=False
            Override memory estimation limits
        username : Optional[str], default=None
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
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        max_iterations: Optional[int] = 10,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        node_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
        write_concurrency: Optional[int] = None,
    ) -> LabelPropagationWriteResult:
        """
        Executes the Label Propagation algorithm and writes the results back to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to write the community IDs to
        concurrency : Optional[int], default=None
            The number of concurrent threads
        consecutive_ids : Optional[bool], default=False
            Whether to use consecutive community IDs starting from 0
        job_id : Optional[str], default=None
            An identifier for the job
        log_progress : bool, default=True
            Whether to log progress
        max_iterations : Optional[int], default=10
            The maximum number of iterations
        min_community_size : Optional[int], default=None
            Minimum community size to include in results
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        node_weight_property : Optional[str], default=None
            The property name for node weights
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : Optional[str], default=None
            The property name for relationship weights
        seed_property : Optional[str], default=None
            The property name containing seed values for initial community assignment
        sudo : Optional[bool] = False
            Override memory estimation limits
        username : Optional[str], default=None
            The username to attribute the procedure run to
        write_concurrency : Optional[int], default=None
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
        G: Union[GraphV2, dict[str, Any]],
        *,
        concurrency: Optional[int] = None,
        consecutive_ids: Optional[bool] = False,
        max_iterations: Optional[int] = 10,
        node_labels: Optional[List[str]] = None,
        node_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        seed_property: Optional[str] = None,
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the Label Propagation algorithm.

        Parameters
        ----------
        G : Union[GraphV2, dict[str, Any]]
            The graph or graph configuration to estimate for
        concurrency : Optional[int], default=None
            The number of concurrent threads
        consecutive_ids : Optional[bool], default=False
            Whether to use consecutive community IDs starting from 0
        max_iterations : Optional[int], default=10
            The maximum number of iterations
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        node_weight_property : Optional[str], default=None
            The property name for node weights
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        relationship_weight_property : Optional[str], default=None
            The property name for relationship weights
        seed_property : Optional[str], default=None
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
