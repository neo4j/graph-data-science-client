from __future__ import annotations

from abc import ABC, abstractmethod

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.similarity.node_similarity_filtered_endpoints import (
    NodeSimilarityFilteredEndpoints,
)
from graphdatascience.procedure_surface.api.similarity.node_similarity_results import (
    NodeSimilarityMutateResult,
    NodeSimilarityStatsResult,
    NodeSimilarityWriteResult,
)


class NodeSimilarityEndpoints(ABC):
    @property
    @abstractmethod
    def filtered(self) -> NodeSimilarityFilteredEndpoints: ...

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        mutate_property: str,
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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> NodeSimilarityMutateResult:
        """
        Runs the Node Similarity algorithm and stores the results as new relationships in the graph catalog.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        mutate_relationship_type : str
            The relationship type to use for the new relationships.
        mutate_property
            Name of the node property to store the results in.
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
            Whether to compute similarity within connected components. Given a string uses the node property stored in the graph
        relationship_weight_property
            Name of the property to be used as weights.
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
        NodeSimilarityMutateResult
            Object containing metadata from the execution.
        """

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> NodeSimilarityStatsResult:
        """
        Runs the Node Similarity algorithm and returns execution statistics.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
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
            Whether to compute similarity within connected components. Given a string uses the node property stored in the graph
        relationship_weight_property
            Name of the property to be used as weights.
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
        NodeSimilarityStatsResult
            Object containing execution statistics and algorithm-specific results.
        """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Runs the Node Similarity algorithm and returns the result as a DataFrame.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
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
            Whether to compute similarity within connected components. Given a string uses the node property stored in the graph
        relationship_weight_property
            Name of the property to be used as weights.
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
            The similarity results as a DataFrame with columns 'node1', 'node2', and 'similarity'.
        """

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        write_property: str,
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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> NodeSimilarityWriteResult:
        """
        Runs the Node Similarity algorithm and writes the results back to the database.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_relationship_type : str
            The relationship type to use for the new relationships.
        write_property
            Name of the node property to store the results in.
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
            Whether to compute similarity within connected components. Given a string uses the node property stored in the graph
        relationship_weight_property
            Name of the property to be used as weights.
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
            Number of concurrent threads to use for writing.Returns
        -------
        NodeSimilarityWriteResult
            Object containing metadata from the execution.
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2,
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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the Node Similarity algorithm.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
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
            Whether to compute similarity within connected components. Given a string uses the node property stored in the graph
        relationship_weight_property
            Name of the property to be used as weights.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.

        Returns
        -------
        EstimationResult
            Object containing the estimated memory requirements.
        """
