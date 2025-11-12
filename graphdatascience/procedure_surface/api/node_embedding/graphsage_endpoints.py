from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.node_embedding.graphsage_predict_endpoints import (
    GraphSageMutateResult,
    GraphSagePredictEndpoints,
    GraphSageWriteResult,
)
from graphdatascience.procedure_surface.api.node_embedding.graphsage_train_endpoints import (
    GraphSageTrainEndpoints,
)


class GraphSageEndpoints(GraphSagePredictEndpoints):
    """
    API for the GraphSage algorithm, combining both training and prediction functionalities.
    """

    def __init__(
        self,
        train_endpoints: GraphSageTrainEndpoints,
        predict_endpoints: GraphSagePredictEndpoints,
    ) -> None:
        self._train_endpoints = train_endpoints
        self._predict_endpoints = predict_endpoints

    @property
    def train(self) -> GraphSageTrainEndpoints:
        """
        Trains a GraphSage model on the given graph.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        model_name : str
            Name under which the model will be stored
        feature_properties : list[str]
            The names of the node properties to use as input features
        activation_function : str | None
            The activation function to apply after each layer
        negative_sample_weight : int | None, default=None
            Weight of negative samples in the loss function
        embedding_dimension : int | None, default=None
            The dimension of the generated embeddings
        tolerance : float
            Tolerance for early stopping based on loss improvement
        learning_rate : float | None, default=None
            Learning rate for the training optimization
        max_iterations : int
            Maximum number of training iterations
        sample_sizes : list[int] | None, default=None
            Number of neighbors to sample at each layer
        aggregator : str | None
            The aggregator function for neighborhood aggregation
        penalty_l2 : float | None, default=None
            L2 regularization penalty
        search_depth : int | None, default=None
            Maximum search depth for neighbor sampling
        epochs : int | None, default=None
            Number of training epochs
        projected_feature_dimension : int | None, default=None
            Dimension to project input features to before training
        batch_sampling_ratio : float | None, default=None
            Ratio of nodes to sample for each training batch
        store_model_to_disk : bool | None, default=None
            Whether to persist the model to disk
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        username : str | None = None
            The username to attribute the procedure run to
        log_progress
            Display progress logging.
        sudo
            Disable the memory guard.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        batch_size : int | None, default=None
            Batch size for training
        relationship_weight_property
            Name of the property to be used as weights.
        random_seed
            Seed for random number generation to ensure reproducible results.

        Returns
        -------
        GraphSageModelV2
            Trained model
        """
        return self._train_endpoints

    def stream(
        self,
        G: GraphV2,
        model_name: str,
        *,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
        batch_size: int = 100,
    ) -> DataFrame:
        return self._predict_endpoints.stream(
            G,
            model_name,
            relationship_types=relationship_types,
            node_labels=node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
            batch_size=batch_size,
        )

    def write(
        self,
        G: GraphV2,
        model_name: str,
        write_property: str,
        *,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        write_concurrency: int | None = None,
        job_id: str | None = None,
        batch_size: int = 100,
    ) -> GraphSageWriteResult:
        return self._predict_endpoints.write(
            G,
            model_name,
            write_property,
            relationship_types=relationship_types,
            node_labels=node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            write_concurrency=write_concurrency,
            job_id=job_id,
            batch_size=batch_size,
        )

    def mutate(
        self,
        G: GraphV2,
        model_name: str,
        mutate_property: str,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
        batch_size: int = 100,
    ) -> GraphSageMutateResult:
        return self._predict_endpoints.mutate(
            G,
            model_name,
            mutate_property,
            relationship_types=relationship_types,
            node_labels=node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
            batch_size=batch_size,
        )

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        model_name: str,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        batch_size: int = 100,
        concurrency: int | None = None,
        log_progress: bool = True,
        username: str | None = None,
        sudo: bool = False,
        job_id: str | None = None,
    ) -> EstimationResult:
        return self._predict_endpoints.estimate(
            G,
            model_name,
            relationship_types=relationship_types,
            node_labels=node_labels,
            batch_size=batch_size,
            concurrency=concurrency,
            log_progress=log_progress,
            username=username,
            sudo=sudo,
            job_id=job_id,
        )
