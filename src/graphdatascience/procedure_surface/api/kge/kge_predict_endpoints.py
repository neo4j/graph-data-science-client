from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.default_values import ALL_TYPES

NodeFilter = int | list[int] | str


class KgePredictEndpoints(ABC):
    """Endpoints for predicting relationships with a knowledge-graph-embedding scoring function.

    Mirrors the ``gds.ml.kge.predict`` procedure. The scoring function (e.g. ``transe`` or
    ``distmult``) and the per-relationship-type embedding are typically supplied by a
    :class:`SimpleRelEmbeddingModel` created via ``gds.kge.transe(...)`` / ``gds.kge.distmult(...)``.
    """

    @abstractmethod
    def stream(
        self,
        G: Graph,
        *,
        node_embedding_property: str,
        relationship_type_embedding: list[float],
        scoring_function: str,
        top_k: int,
        source_node_filter: NodeFilter | None = None,
        target_node_filter: NodeFilter | None = None,
        relationship_types: list[str] = ALL_TYPES,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        """Compute and stream the ``top_k`` highest scoring target nodes for each source node.

        Parameters
        ----------
        G
            Graph object to use.
        node_embedding_property
            The name of the node property storing the embeddings in the graph.
        relationship_type_embedding
            The embedding vector of the relationship type used in the computation.
        scoring_function
            The scoring function to use, e.g. ``transe`` or ``distmult``.
        top_k
            How many target nodes to return for each source node.
        source_node_filter
            The specification of source nodes to consider.
        target_node_filter
            The specification of target nodes to consider.
        relationship_types
            Filter the graph using the given relationship types.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.

        Returns
        -------
        DataFrame
            The ``top_k`` highest scoring target nodes for each source node, with the score for the node pair.
        """

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        *,
        node_embedding_property: str,
        relationship_type_embedding: list[float],
        scoring_function: str,
        top_k: int,
        mutate_relationship_type: str,
        mutate_property: str = "score",
        source_node_filter: NodeFilter | None = None,
        target_node_filter: NodeFilter | None = None,
        relationship_types: list[str] = ALL_TYPES,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        username: str | None = None,
    ) -> KgeMutateResult:
        """Compute relationships and add them to the in-memory graph under a new relationship type.

        Parameters
        ----------
        G
            Graph object to use.
        node_embedding_property
            The name of the node property storing the embeddings in the graph.
        relationship_type_embedding
            The embedding vector of the relationship type used in the computation.
        scoring_function
            The scoring function to use, e.g. ``transe`` or ``distmult``.
        top_k
            How many relationships to add for each source node.
        mutate_relationship_type
            The relationship type for the predicted relationships.
        mutate_property
            The property on the new relationships storing the prediction score.
        source_node_filter
            The specification of source nodes to consider.
        target_node_filter
            The specification of target nodes to consider.
        relationship_types
            Filter the graph using the given relationship types.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.

        Returns
        -------
        KgeMutateResult
            Metadata about the performed computation and mutation.
        """

    @abstractmethod
    def write(
        self,
        G: Graph,
        *,
        node_embedding_property: str,
        relationship_type_embedding: list[float],
        scoring_function: str,
        top_k: int,
        write_relationship_type: str,
        write_property: str = "score",
        source_node_filter: NodeFilter | None = None,
        target_node_filter: NodeFilter | None = None,
        relationship_types: list[str] = ALL_TYPES,
        concurrency: int | None = None,
        write_concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        username: str | None = None,
    ) -> KgeWriteResult:
        """Compute relationships and write them back to the database under a new relationship type.

        Parameters
        ----------
        G
            Graph object to use.
        node_embedding_property
            The name of the node property storing the embeddings in the graph.
        relationship_type_embedding
            The embedding vector of the relationship type used in the computation.
        scoring_function
            The scoring function to use, e.g. ``transe`` or ``distmult``.
        top_k
            How many relationships to add for each source node.
        write_relationship_type
            The relationship type for the predicted relationships.
        write_property
            The property on the new relationships storing the prediction score.
        source_node_filter
            The specification of source nodes to consider.
        target_node_filter
            The specification of target nodes to consider.
        relationship_types
            Filter the graph using the given relationship types.
        concurrency
            Number of concurrent threads to use.
        write_concurrency
            Number of concurrent threads used for writing.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.

        Returns
        -------
        KgeWriteResult
            Metadata about the performed computation and write-back.
        """


class KgeMutateResult(BaseResult):
    relationships_written: int
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    post_processing_millis: int
    configuration: dict[str, Any]


class KgeWriteResult(BaseResult):
    relationships_written: int
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    configuration: dict[str, Any]
