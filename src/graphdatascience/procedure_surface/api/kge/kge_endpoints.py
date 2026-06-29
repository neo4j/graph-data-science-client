from __future__ import annotations

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.kge.kge_predict_endpoints import KgePredictEndpoints
from graphdatascience.procedure_surface.api.kge.simple_rel_embedding_model import SimpleRelEmbeddingModel


class KgeEndpoints:
    """Endpoints for knowledge-graph-embedding relationship prediction."""

    def __init__(self, predict_endpoints: KgePredictEndpoints) -> None:
        self._predict_endpoints = predict_endpoints

    @property
    def predict(self) -> KgePredictEndpoints:
        """Flat prediction endpoints, taking the scoring function as an explicit parameter."""
        return self._predict_endpoints

    def transe(
        self,
        G: Graph,
        node_embedding_property: str,
        relationship_type_embeddings: dict[str, list[float]],
    ) -> SimpleRelEmbeddingModel:
        """Create a TransE relationship-embedding model over the given graph.

        Parameters
        ----------
        G
            Graph object to use.
        node_embedding_property
            The name of the node property storing the embeddings in the graph.
        relationship_type_embeddings
            A map from relationship type to its embedding vector.

        Returns
        -------
        SimpleRelEmbeddingModel
            A model using the TransE scoring function.
        """
        return SimpleRelEmbeddingModel(
            "transe", G, node_embedding_property, relationship_type_embeddings, self._predict_endpoints
        )

    def distmult(
        self,
        G: Graph,
        node_embedding_property: str,
        relationship_type_embeddings: dict[str, list[float]],
    ) -> SimpleRelEmbeddingModel:
        """Create a DistMult relationship-embedding model over the given graph.

        Parameters
        ----------
        G
            Graph object to use.
        node_embedding_property
            The name of the node property storing the embeddings in the graph.
        relationship_type_embeddings
            A map from relationship type to its embedding vector.

        Returns
        -------
        SimpleRelEmbeddingModel
            A model using the DistMult scoring function.
        """
        return SimpleRelEmbeddingModel(
            "distmult", G, node_embedding_property, relationship_type_embeddings, self._predict_endpoints
        )
