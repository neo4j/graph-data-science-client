from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class AStarWriteResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    relationships_written: int
    configuration: dict[str, Any]


class AStarMutateResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    relationships_written: int
    configuration: dict[str, Any]


class SourceTargetAStarEndpoints(ABC):
    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        source_node: int,
        target_node: int,
        latitude_property: str,
        longitude_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Runs the A* shortest path algorithm and returns the result as a DataFrame.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on.
        source_node : int
            The source node for the shortest path computation.
        target_node : int
            The target node for the shortest path computation.
        latitude_property : str
            The node property that stores latitude values.
        longitude_property : str
            The node property that stores longitude values.
        relationship_weight_property : str | None, default=None
            The relationship property to use as weights.
        relationship_types : list[str] | None, default=None
            Filter on relationship types.
        node_labels : list[str] | None, default=None
            Filter on node labels.
        sudo : bool, default=False
            Run the algorithm with elevated privileges.
        log_progress : bool, default=True
            Whether to log progress.
        username : str | None, default=None
            Username for the operation.
        concurrency : int | None, default=None
            Concurrency configuration.
        job_id : str | None, default=None
            Job ID for the operation.

        Returns
        -------
        DataFrame
            The shortest path results as a DataFrame with columns for sourceNode, targetNode, totalCost, nodeIds, costs, index.
        """

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        source_node: int,
        target_node: int,
        latitude_property: str,
        longitude_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> AStarMutateResult:
        """
        Runs the A* shortest path algorithm and stores the results as new relationships in the graph catalog.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on.
        mutate_relationship_type : str
            The relationship type to use for the new relationships in the graph catalog.
        source_node : int
            The source node for the shortest path computation.
        target_node : int
            The target node for the shortest path computation.
        latitude_property : str
            The node property that stores latitude values.
        longitude_property : str
            The node property that stores longitude values.
        relationship_weight_property : str | None, default=None
            The relationship property to use as weights.
        relationship_types : list[str] | None, default=None
            Filter on relationship types.
        node_labels : list[str] | None, default=None
            Filter on node labels.
        sudo : bool, default=False
            Run the algorithm with elevated privileges.
        log_progress : bool, default=True
            Whether to log progress.
        username : str | None, default=None
            Username for the operation.
        concurrency : int | None, default=None
            Concurrency configuration.
        job_id : str | None, default=None
            Job ID for the operation.

        Returns
        -------
        AStarMutateResult
            Object containing metadata from the execution.
        """

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        source_node: int,
        target_node: int,
        latitude_property: str,
        longitude_property: str,
        write_node_ids: bool = False,
        write_costs: bool = False,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> AStarWriteResult:
        """
        Runs the A* shortest path algorithm and writes the results back to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on.
        write_relationship_type : str
            The relationship type to use for the new relationships.
        source_node : int
            The source node for the shortest path computation.
        target_node : int
            The target node for the shortest path computation.
        latitude_property : str
            The node property that stores latitude values.
        longitude_property : str
            The node property that stores longitude values.
        write_node_ids : bool, default=False
            Whether to write node IDs of the shortest path onto the relationship.
        write_costs : bool, default=False
            Whether to write costs of the shortest path onto the relationship.
        relationship_weight_property : str | None, default=None
            The relationship property to use as weights.
        relationship_types : list[str] | None, default=None
            Filter on relationship types.
        node_labels : list[str] | None, default=None
            Filter on node labels.
        sudo : bool, default=False
            Run the algorithm with elevated privileges.
        log_progress : bool, default=True
            Whether to log progress.
        username : str | None, default=None
            Username for the operation.
        concurrency : int | None, default=None
            Concurrency configuration.
        job_id : str | None, default=None
            Job ID for the operation.
        write_concurrency : int | None, default=None
            Concurrency for writing results.

        Returns
        -------
        AStarWriteResult
            Object containing metadata from the execution.
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        source_node: int,
        target_node: int,
        latitude_property: str,
        longitude_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the A* shortest path algorithm.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on.
        source_node : int
            The source node for the shortest path computation.
        target_node : int
            The target node for the shortest path computation.
        latitude_property : str
            The node property that stores latitude values.
        longitude_property : str
            The node property that stores longitude values.
        relationship_weight_property : str | None, default=None
            The relationship property to use as weights.
        relationship_types : list[str] | None, default=None
            Filter on relationship types.
        node_labels : list[str] | None, default=None
            Filter on node labels.
        sudo : bool, default=False
            Run the algorithm with elevated privileges.
        username : str | None, default=None
            Username for the operation.
        concurrency : int | None, default=None
            Concurrency configuration.

        Returns
        -------
        EstimationResult
            Object containing the estimated memory requirements.
        """
