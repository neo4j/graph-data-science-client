from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class ClosenessEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        use_wasserman_faust: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> ClosenessMutateResult:
        """
        Runs the Closeness Centrality algorithm and stores the results in the graph catalog as a new node property.

        Closeness centrality is a way of detecting nodes that are able to spread information very efficiently through a graph.
        The closeness centrality of a node measures its average farness (inverse distance) to all other nodes.
        Nodes with a high closeness score have the shortest distances to all other nodes.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        mutate_property : str
            The property name to store the closeness centrality score for each node
        use_wasserman_faust : bool
            Use the improved Wasserman-Faust formula for closeness computation.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress : bool, default=True
            Whether to log progress of the algorithm execution
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            The number of concurrent threads used for the algorithm execution.
        job_id : str | None
            An identifier for the job that can be used for monitoring and cancellation

        Returns
        -------
        ClosenessMutateResult
            Algorithm metrics and statistics including the centrality distribution
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        use_wasserman_faust: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> ClosenessStatsResult:
        """
        Runs the Closeness Centrality algorithm and returns result statistics without storing the results.

        Closeness centrality is a way of detecting nodes that are able to spread information very efficiently through a graph.
        The closeness centrality of a node measures its average farness (inverse distance) to all other nodes.
        Nodes with a high closeness score have the shortest distances to all other nodes.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        use_wasserman_faust : bool
            Use the improved Wasserman-Faust formula for closeness computation.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress : bool, default=True
            Whether to log progress of the algorithm execution
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            The number of concurrent threads used for the algorithm execution.
        job_id : str | None
            An identifier for the job that can be used for monitoring and cancellation

        Returns
        -------
        ClosenessStatsResult
            Algorithm statistics including the centrality distribution
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        use_wasserman_faust: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Executes the Closeness Centrality algorithm and returns a stream of results.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        use_wasserman_faust : bool
            Use the improved Wasserman-Faust formula for closeness computation.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress : bool, default=True
            Whether to log progress of the algorithm execution
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None, default=None
            The number of concurrent threads used for the algorithm execution.
        job_id : str | None, default=None
            An identifier for the job that can be used for monitoring and cancellation

        Returns
        -------
        DataFrame
            DataFrame with nodeId and score columns containing closeness centrality results.
            Each row represents a node with its corresponding closeness centrality score.
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
        use_wasserman_faust: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> ClosenessWriteResult:
        """
        Runs the Closeness Centrality algorithm and stores the result in the Neo4j database as a new node property.

        Closeness centrality is a way of detecting nodes that are able to spread information very efficiently through a graph.
        The closeness centrality of a node measures its average farness (inverse distance) to all other nodes.
        Nodes with a high closeness score have the shortest distances to all other nodes.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_property : str
            The property name to write closeness centrality scores to in the Neo4j database
        use_wasserman_faust : bool | None, default=None
            Use the improved Wasserman-Faust formula for closeness computation.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress : bool, default=True
            Whether to log progress of the algorithm execution
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            The number of concurrent threads used for the algorithm execution.
        job_id : str | None
            An identifier for the job that can be used for monitoring and cancellation
        write_concurrency : int | None
            The number of concurrent threads used during the write phase.

        Returns
        -------
        ClosenessWriteResult
            Algorithm metrics and statistics including the number of properties written
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        use_wasserman_faust: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on or a dictionary representing the graph configuration.
        use_wasserman_faust : bool | None, default=None
            Use the improved Wasserman-Faust formula for closeness computation.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency : int | None
            The number of concurrent threads used for the algorithm execution.

        Returns
        -------
        EstimationResult
            An object containing the result of the estimation
        """
        pass


class ClosenessMutateResult(BaseResult):
    """Result of running Closeness Centrality algorithm with mutate mode."""

    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    mutate_millis: int
    centrality_distribution: dict[str, Any]
    configuration: dict[str, Any]


class ClosenessStatsResult(BaseResult):
    """Result of running Closeness Centrality algorithm with stats mode."""

    centrality_distribution: dict[str, Any]
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class ClosenessWriteResult(BaseResult):
    """Result of running Closeness Centrality algorithm with write mode."""

    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    post_processing_millis: int
    write_millis: int
    centrality_distribution: dict[str, Any]
    configuration: dict[str, Any]
