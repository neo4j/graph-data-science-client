from __future__ import annotations

from typing import TYPE_CHECKING

from pandas import DataFrame

from graphdatascience.graph.graph_api import Graph
from graphdatascience.model.model import Model
from graphdatascience.model.model_catalog_protocol import ModelCatalogProtocol
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.node_embedding.graphsage_results import (
    GraphSageRuntimeMutateResult,
    GraphSageRuntimeWriteResult,
)

if TYPE_CHECKING:
    from graphdatascience.procedure_surface.api.node_embedding.graphsage_unsupervised_endpoints import (
        GraphSageUnsupervisedEndpoints,
    )


class GraphSageUnsupervisedModel(Model):
    """
    Represents an unsupervised GraphSage model in the model catalog.
    Construct this using `gds.graph_sage.unsupervised.train`.
    """

    def __init__(
        self, name: str, catalog: ModelCatalogProtocol, predict_endpoints: GraphSageUnsupervisedEndpoints
    ) -> None:
        super().__init__(name, catalog)
        self._predict_endpoints = predict_endpoints

    def predict_stream(
        self,
        G: Graph,
        feature_properties: list[str],
        *,
        batch_size: int | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Generate embeddings for the given graph and stream the results.

        Parameters
        ----------
        G
            The graph to generate embeddings for.
        feature_properties
            Names of the node properties to use as input features
        batch_size
            Number of nodes to process in each batch.
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        job_id
            Identifier for the computation.

        Returns
        -------
        pandas.DataFrame
            The streaming results as a DataFrame.
        """
        return self._predict_endpoints.stream(
            G,
            model_name=self.name(),
            feature_properties=feature_properties,
            batch_size=batch_size,
            random_seed=random_seed,
            relationship_types=relationship_types,
            node_labels=node_labels,
            job_id=job_id,
        )

    def predict_write(
        self,
        G: Graph,
        feature_properties: list[str],
        write_property: str,
        *,
        batch_size: int | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> GraphSageRuntimeWriteResult:
        """
        Generate embeddings for the given graph and write the results to the database.

        Parameters
        ----------
        G
            The graph to generate embeddings for.
        feature_properties
            Names of the node properties to use as input features
        write_property
            The property to write the embeddings to.
        batch_size
            Number of nodes to process in each batch.
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        job_id
            Identifier for the computation.
        write_concurrency
            Number of concurrent threads to use for writing.

        Returns
        -------
        GraphSageRuntimeWriteResult
            The result of the write operation.
        """
        return self._predict_endpoints.write(
            G,
            model_name=self.name(),
            feature_properties=feature_properties,
            write_property=write_property,
            batch_size=batch_size,
            random_seed=random_seed,
            relationship_types=relationship_types,
            node_labels=node_labels,
            job_id=job_id,
            write_concurrency=write_concurrency,
        )

    def predict_mutate(
        self,
        G: Graph,
        feature_properties: list[str],
        mutate_property: str,
        *,
        batch_size: int | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        job_id: str | None = None,
    ) -> GraphSageRuntimeMutateResult:
        """
        Generate embeddings for the given graph and mutate the graph with the results.

        Parameters
        ----------
        G
            The graph to generate embeddings for.
        feature_properties
            Names of the node properties to use as input features
        mutate_property
            The property to mutate with the embeddings.
        batch_size
            Number of nodes to process in each batch.
        random_seed
            Seed for random number generation to ensure reproducible results.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        job_id
            Identifier for the computation.

        Returns
        -------
        GraphSageRuntimeMutateResult
            The result of the mutate operation.
        """
        return self._predict_endpoints.mutate(
            G,
            model_name=self.name(),
            feature_properties=feature_properties,
            mutate_property=mutate_property,
            batch_size=batch_size,
            random_seed=random_seed,
            relationship_types=relationship_types,
            node_labels=node_labels,
            job_id=job_id,
        )
