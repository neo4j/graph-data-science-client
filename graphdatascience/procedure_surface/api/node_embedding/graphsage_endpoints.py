from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
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
        G : GraphV2
            The graph to run the algorithm on
        model_name : str
            Name under which the model will be stored
        feature_properties : list[str]
            The names of the node properties to use as input features
        activation_function : Any | None, default=None
            The activation function to apply after each layer
        negative_sample_weight : int | None, default=None
            Weight of negative samples in the loss function
        embedding_dimension : int | None, default=None
            The dimension of the generated embeddings
        tolerance : float | None, default=None
            Tolerance for early stopping based on loss improvement
        learning_rate : float | None, default=None
            Learning rate for the training optimization
        max_iterations : int | None, default=None
            Maximum number of training iterations
        sample_sizes : list[int] | None, default=None
            Number of neighbors to sample at each layer
        aggregator : Any | None, default=None
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
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        username : str | None = None
            The username to attribute the procedure run to
        log_progress : bool | None, default=None
            Whether to log progress
        sudo : bool | None, default=None
            Override memory estimation limits
        concurrency : Any | None, default=None
            The number of concurrent threads
        job_id : Any | None, default=None
            An identifier for the job
        batch_size : int | None, default=None
            Batch size for training
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        random_seed : Any | None, default=None
            Random seed for reproducible results

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
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        batch_size: int | None = None,
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
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        write_concurrency: int | None = None,
        job_id: Any | None = None,
        batch_size: int | None = None,
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
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        batch_size: int | None = None,
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
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        batch_size: int | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        username: str | None = None,
        sudo: bool | None = None,
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
