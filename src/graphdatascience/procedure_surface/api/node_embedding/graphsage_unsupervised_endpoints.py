from __future__ import annotations

from abc import ABC, abstractmethod

from pandas import DataFrame

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.node_embedding.graphsage_runtime_model import (
    GraphSageRuntimeMutateResult,
    GraphSageRuntimeTrainResult,
    GraphSageRuntimeWriteResult,
    GraphSageUnsupervisedModel,
)


class GraphSageUnsupervisedEndpoints(ABC):
    """
    Endpoints for the unsupervised GraphSage algorithm.

    This is the successor of the classic `gds.graph_sage.train` endpoint.
    """

    @abstractmethod
    def train(
        self,
        G: Graph,
        model_name: str,
        feature_properties: list[str],
        *,
        epochs: int,
        activation_function: str = "relu",
        aggregator: str = "mean",
        batch_size: int | None = None,
        dropout: float = 0.1,
        embedding_dimension: int = 256,
        layer_normalization: bool = True,
        learning_rate: float = 0.001,
        negative_sampling_ratio: float = 1.0,
        num_neighbors: list[int] = [20, 10],
        num_walks: int = 10,
        walk_depth: int = 3,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        job_id: str | None = None,
    ) -> tuple[GraphSageUnsupervisedModel, GraphSageRuntimeTrainResult]:
        """
        Trains an unsupervised GraphSage model on the given graph.

        This is the successor of the classic `gds.graph_sage.train` endpoint. The resulting model can be used for
        prediction with `gds.graph_sage.unsupervised.stream`, `.write` and `.mutate`.

        Parameters
        ----------
        G
           Graph object to use
        model_name
            Name of the trained model.
        feature_properties
            Names of the node properties to use as input features
        epochs
            Maximum number of training epochs.
        activation_function
            The activation function to apply after each layer
        aggregator
            The aggregator function for neighborhood aggregation
        batch_size
            Number of nodes to process in each batch.
        dropout
            Dropout probability applied during training.
        embedding_dimension
            Output dimensionality of the embeddings
        layer_normalization
            Whether to apply layer normalization after each layer
        learning_rate
            Learning rate for the training optimization
        negative_sampling_ratio
            Ratio of negative to positive samples for training
        num_neighbors
            Number of neighbors to sample at each layer
        num_walks
            Number of random walks to generate per node
        walk_depth
            Depth of the random walks used for training
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
        tuple[GraphSageUnsupervisedModel, GraphSageRuntimeTrainResult]
            The trained model and training metrics
        """

    @abstractmethod
    def stream(
        self,
        G: Graph,
        model_name: str,
        feature_properties: list[str],
        *,
        batch_size: int | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Uses a pre-trained unsupervised GraphSage model to predict embeddings for a graph and returns the results as a stream.

        Parameters
        ----------
        G
           Graph object to use
        model_name
            Name under which the model is stored
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
            DataFrame with node IDs and their embeddings
        """

    @abstractmethod
    def write(
        self,
        G: Graph,
        model_name: str,
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
        Uses a pre-trained unsupervised GraphSage model to predict embeddings for a graph and writes the results back to the database.

        Parameters
        ----------
        G
           Graph object to use
        model_name
            Name under which the model is stored
        feature_properties
            Names of the node properties to use as input features
        write_property
            Name of the node property to store the results in.
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
            Algorithm metrics and statistics
        """

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        model_name: str,
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
        Uses a pre-trained unsupervised GraphSage model to predict embeddings for a graph and writes the results back to the graph as a node property.

        Parameters
        ----------
        G
           Graph object to use
        model_name
            Name under which the model is stored
        feature_properties
            Names of the node properties to use as input features
        mutate_property
            Name of the node property to store the results in.
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
            Algorithm metrics and statistics
        """
