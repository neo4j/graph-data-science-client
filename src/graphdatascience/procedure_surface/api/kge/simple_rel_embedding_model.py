from __future__ import annotations

from pandas import DataFrame

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_TYPES
from graphdatascience.procedure_surface.api.kge.kge_predict_endpoints import (
    KgeMutateResult,
    KgePredictEndpoints,
    KgeWriteResult,
    NodeFilter,
)


class SimpleRelEmbeddingModel:
    """A client-side model for computing and ranking pairwise scores between nodes according to a
    knowledge-graph-embedding scoring function (e.g. TransE or DistMult).

    It can also produce new relationships based on these rankings. Instances are not stored in the
    model catalog; construct them via ``gds.kge.transe(...)`` or ``gds.kge.distmult(...)``.
    """

    def __init__(
        self,
        scoring_function: str,
        graph: Graph,
        node_embedding_property: str,
        relationship_type_embeddings: dict[str, list[float]],
        predict_endpoints: KgePredictEndpoints,
    ) -> None:
        self._scoring_function = scoring_function
        self._graph = graph
        self._node_embedding_property = node_embedding_property
        self._relationship_type_embeddings = relationship_type_embeddings
        self._predict_endpoints = predict_endpoints

    def predict_stream(
        self,
        source_node_filter: NodeFilter,
        target_node_filter: NodeFilter,
        relationship_type: str,
        top_k: int,
        *,
        relationship_types: list[str] = ALL_TYPES,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
    ) -> DataFrame:
        """Compute and stream the ``top_k`` highest scoring target nodes for each source node.

        Parameters
        ----------
        source_node_filter
            The specification of source nodes to consider.
        target_node_filter
            The specification of target nodes to consider.
        relationship_type
            The relationship type whose embedding will be used in the computation.
        top_k
            How many target nodes to return for each source node.
        relationship_types
            Filter the graph using the given relationship types.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.

        Returns
        -------
        DataFrame
            The ``top_k`` highest scoring target nodes for each source node, with the score for the node pair.
        """
        return self._predict_endpoints.stream(
            self._graph,
            source_node_filter=source_node_filter,
            target_node_filter=target_node_filter,
            node_embedding_property=self._node_embedding_property,
            relationship_type_embedding=self._relationship_type_embeddings[relationship_type],
            scoring_function=self._scoring_function,
            top_k=top_k,
            relationship_types=relationship_types,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
        )

    def predict_mutate(
        self,
        source_node_filter: NodeFilter,
        target_node_filter: NodeFilter,
        relationship_type: str,
        top_k: int,
        mutate_relationship_type: str,
        mutate_property: str,
        *,
        relationship_types: list[str] = ALL_TYPES,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
    ) -> KgeMutateResult:
        """Compute relationships and add them to the in-memory graph under a new relationship type.

        Parameters
        ----------
        source_node_filter
            The specification of source nodes to consider.
        target_node_filter
            The specification of target nodes to consider.
        relationship_type
            The relationship type whose embedding will be used in the computation.
        top_k
            How many relationships to add for each source node.
        mutate_relationship_type
            The relationship type for the predicted relationships.
        mutate_property
            The property on the new relationships storing the prediction score.
        relationship_types
            Filter the graph using the given relationship types.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.

        Returns
        -------
        KgeMutateResult
            Metadata about the performed computation and mutation.
        """
        return self._predict_endpoints.mutate(
            self._graph,
            source_node_filter=source_node_filter,
            target_node_filter=target_node_filter,
            node_embedding_property=self._node_embedding_property,
            relationship_type_embedding=self._relationship_type_embeddings[relationship_type],
            scoring_function=self._scoring_function,
            top_k=top_k,
            mutate_relationship_type=mutate_relationship_type,
            mutate_property=mutate_property,
            relationship_types=relationship_types,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
        )

    def predict_write(
        self,
        source_node_filter: NodeFilter,
        target_node_filter: NodeFilter,
        relationship_type: str,
        top_k: int,
        write_relationship_type: str,
        write_property: str,
        *,
        relationship_types: list[str] = ALL_TYPES,
        concurrency: int | None = None,
        write_concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
    ) -> KgeWriteResult:
        """Compute relationships and write them back to the database under a new relationship type.

        Parameters
        ----------
        source_node_filter
            The specification of source nodes to consider.
        target_node_filter
            The specification of target nodes to consider.
        relationship_type
            The relationship type whose embedding will be used in the computation.
        top_k
            How many relationships to add for each source node.
        write_relationship_type
            The relationship type for the predicted relationships.
        write_property
            The property on the new relationships storing the prediction score.
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

        Returns
        -------
        KgeWriteResult
            Metadata about the performed computation and write-back.
        """
        return self._predict_endpoints.write(
            self._graph,
            source_node_filter=source_node_filter,
            target_node_filter=target_node_filter,
            node_embedding_property=self._node_embedding_property,
            relationship_type_embedding=self._relationship_type_embeddings[relationship_type],
            scoring_function=self._scoring_function,
            top_k=top_k,
            write_relationship_type=write_relationship_type,
            write_property=write_property,
            relationship_types=relationship_types,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
        )

    def scoring_function(self) -> str:
        """Return the name of the scoring function the model uses."""
        return self._scoring_function

    def graph(self) -> Graph:
        """Return the graph the model is based on."""
        return self._graph

    def node_embedding_property(self) -> str:
        """Return the name of the node property storing embeddings in the graph."""
        return self._node_embedding_property

    def relationship_type_embeddings(self) -> dict[str, list[float]]:
        """Return the relationship type embeddings of the model."""
        return self._relationship_type_embeddings
