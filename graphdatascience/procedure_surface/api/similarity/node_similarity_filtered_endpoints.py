from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.similarity.node_similarity_results import (
    NodeSimilarityMutateResult,
    NodeSimilarityStatsResult,
    NodeSimilarityWriteResult,
)


class NodeSimilarityFilteredEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        mutate_property: str,
        source_node_filter: str | list[int],
        target_node_filter: str | list[int],
        top_k: int = 10,
        bottom_k: int = 10,
        top_n: int = 0,
        bottom_n: int = 0,
        similarity_cutoff: float = 1.0e-42,
        degree_cutoff: int = 1,
        upper_degree_cutoff: int = 2147483647,
        similarity_metric: str = "JACCARD",
        use_components: bool | str = False,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> NodeSimilarityMutateResult:
        """
        Runs the Filtered Node Similarity algorithm and stores the results as new relationships in the graph catalog.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_relationship_type : str
            The relationship type to use for the new relationships.
        mutate_property : str
            The relationship property to store the similarity score in.
        source_node_filter : str | list[int]
            A Cypher expression or list of node IDs to filter which nodes can be sources.
        target_node_filter : str | list[int]
            A Cypher expression or list of node IDs to filter which nodes can be targets.
        top_k : int, default=10
            The maximum number of neighbors with the highest similarity scores to compute per node.
        bottom_k : int, default=10
            The maximum number of neighbors with the lowest similarity scores to compute per node.
        top_n : int, default=0
            The maximum number of neighbors to select globally based on similarity scores.
        bottom_n : int, default=0
            The maximum number of neighbors to select globally based on lowest similarity scores.
        similarity_cutoff : float, default=1.0e-42
            The threshold for similarity scores.
        degree_cutoff : int, default=1
            The minimum degree a node must have to be considered.
        upper_degree_cutoff : int, default=2147483647
            The maximum degree a node can have to be considered.
        similarity_metric : str, default="JACCARD"
            The similarity metric to use for computation.
        use_components : bool | str, default=False
            Whether to compute similarity within connected components.
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
        NodeSimilarityMutateResult
            Object containing metadata from the execution.
        """

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        source_node_filter: str | list[int],
        target_node_filter: str | list[int],
        top_k: int = 10,
        bottom_k: int = 10,
        top_n: int = 0,
        bottom_n: int = 0,
        similarity_cutoff: float = 1.0e-42,
        degree_cutoff: int = 1,
        upper_degree_cutoff: int = 2147483647,
        similarity_metric: str = "JACCARD",
        use_components: bool | str = False,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> NodeSimilarityStatsResult:
        """
        Runs the Filtered Node Similarity algorithm and returns execution statistics.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        source_node_filter : str | list[int]
            A Cypher expression or list of node IDs to filter which nodes can be sources.
        target_node_filter : str | list[int]
            A Cypher expression or list of node IDs to filter which nodes can be targets.
        top_k : int, default=10
            The maximum number of neighbors with the highest similarity scores to compute per node.
        bottom_k : int, default=10
            The maximum number of neighbors with the lowest similarity scores to compute per node.
        top_n : int, default=0
            The maximum number of neighbors to select globally based on similarity scores.
        bottom_n : int, default=0
            The maximum number of neighbors to select globally based on lowest similarity scores.
        similarity_cutoff : float, default=1.0e-42
            The threshold for similarity scores.
        degree_cutoff : int, default=1
            The minimum degree a node must have to be considered.
        upper_degree_cutoff : int, default=2147483647
            The maximum degree a node can have to be considered.
        similarity_metric : str, default="JACCARD"
            The similarity metric to use for computation.
        use_components : bool | str, default=False
            Whether to compute similarity within connected components.
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
        NodeSimilarityStatsResult
            Object containing execution statistics and algorithm-specific results.
        """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        source_node_filter: str | list[int],
        target_node_filter: str | list[int],
        top_k: int = 10,
        bottom_k: int = 10,
        top_n: int = 0,
        bottom_n: int = 0,
        similarity_cutoff: float = 1.0e-42,
        degree_cutoff: int = 1,
        upper_degree_cutoff: int = 2147483647,
        similarity_metric: str = "JACCARD",
        use_components: bool | str = False,
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
        Runs the Filtered Node Similarity algorithm and returns the result as a DataFrame.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        source_node_filter : str | list[int]
            A Cypher expression or list of node IDs to filter which nodes can be sources.
        target_node_filter : str | list[int]
            A Cypher expression or list of node IDs to filter which nodes can be targets.
        top_k : int, default=10
            The maximum number of neighbors with the highest similarity scores to compute per node.
        bottom_k : int, default=10
            The maximum number of neighbors with the lowest similarity scores to compute per node.
        top_n : int, default=0
            The maximum number of neighbors to select globally based on similarity scores.
        bottom_n : int, default=0
            The maximum number of neighbors to select globally based on lowest similarity scores.
        similarity_cutoff : float, default=1.0e-42
            The threshold for similarity scores.
        degree_cutoff : int, default=1
            The minimum degree a node must have to be considered.
        upper_degree_cutoff : int, default=2147483647
            The maximum degree a node can have to be considered.
        similarity_metric : str, default="JACCARD"
            The similarity metric to use for computation.
        use_components : bool | str, default=False
            Whether to compute similarity within connected components.
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
            The similarity results as a DataFrame with columns 'node1', 'node2', and 'similarity'.
        """

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        write_property: str,
        source_node_filter: str | list[int],
        target_node_filter: str | list[int],
        top_k: int = 10,
        bottom_k: int = 10,
        top_n: int = 0,
        bottom_n: int = 0,
        similarity_cutoff: float = 1.0e-42,
        degree_cutoff: int = 1,
        upper_degree_cutoff: int = 2147483647,
        similarity_metric: str = "JACCARD",
        use_components: bool | str = False,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> NodeSimilarityWriteResult:
        """
        Runs the Filtered Node Similarity algorithm and writes the results back to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_relationship_type : str
            The relationship type to use for the new relationships.
        write_property : str
            The relationship property to store the similarity score in.
        source_node_filter : str | list[int]
            A Cypher expression or list of node IDs to filter which nodes can be sources.
        target_node_filter : str | list[int]
            A Cypher expression or list of node IDs to filter which nodes can be targets.
        top_k : int, default=10
            The maximum number of neighbors with the highest similarity scores to compute per node.
        bottom_k : int, default=10
            The maximum number of neighbors with the lowest similarity scores to compute per node.
        top_n : int, default=0
            The maximum number of neighbors to select globally based on similarity scores.
        bottom_n : int, default=0
            The maximum number of neighbors to select globally based on lowest similarity scores.
        similarity_cutoff : float, default=1.0e-42
            The threshold for similarity scores.
        degree_cutoff : int, default=1
            The minimum degree a node must have to be considered.
        upper_degree_cutoff : int, default=2147483647
            The maximum degree a node can have to be considered.
        similarity_metric : str, default="JACCARD"
            The similarity metric to use for computation.
        use_components : bool | str, default=False
            Whether to compute similarity within connected components.
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
        NodeSimilarityWriteResult
            Object containing metadata from the execution.
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        source_node_filter: str | list[int],
        target_node_filter: str | list[int],
        top_k: int = 10,
        bottom_k: int = 10,
        top_n: int = 0,
        bottom_n: int = 0,
        similarity_cutoff: float = 1.0e-42,
        degree_cutoff: int = 1,
        upper_degree_cutoff: int = 2147483647,
        similarity_metric: str = "JACCARD",
        use_components: bool | str = False,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the Filtered Node Similarity algorithm.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on.
        source_node_filter : str | list[int]
            A Cypher expression or list of node IDs to filter which nodes can be sources.
        target_node_filter : str | list[int]
            A Cypher expression or list of node IDs to filter which nodes can be targets.
        top_k : int, default=10
            The maximum number of neighbors with the highest similarity scores to compute per node.
        bottom_k : int, default=10
            The maximum number of neighbors with the lowest similarity scores to compute per node.
        top_n : int, default=0
            The maximum number of neighbors to select globally based on similarity scores.
        bottom_n : int, default=0
            The maximum number of neighbors to select globally based on lowest similarity scores.
        similarity_cutoff : float, default=1.0e-42
            The threshold for similarity scores.
        degree_cutoff : int, default=1
            The minimum degree a node must have to be considered.
        upper_degree_cutoff : int, default=2147483647
            The maximum degree a node can have to be considered.
        similarity_metric : str, default="JACCARD"
            The similarity metric to use for computation.
        use_components : bool | str, default=False
            Whether to compute similarity within connected components.
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
