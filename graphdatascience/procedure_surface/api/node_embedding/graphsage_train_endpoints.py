from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.model.graphsage_model import GraphSageModelV2


class GraphSageTrainEndpoints(ABC):
    @abstractmethod
    def __call__(
        self,
        G: GraphV2,
        model_name: str,
        feature_properties: list[str],
        *,
        activation_function: Any | None = None,
        negative_sample_weight: int | None = None,
        embedding_dimension: int | None = None,
        tolerance: float | None = None,
        learning_rate: float | None = None,
        max_iterations: int | None = None,
        sample_sizes: list[int] | None = None,
        aggregator: Any | None = None,
        penalty_l2: float | None = None,
        search_depth: int | None = None,
        epochs: int | None = None,
        projected_feature_dimension: int | None = None,
        batch_sampling_ratio: float | None = None,
        store_model_to_disk: bool | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        batch_size: int | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> tuple[GraphSageModelV2, GraphSageTrainResult]: ...

    @abstractmethod
    def estimate(
        self,
        G: GraphV2,
        model_name: str,
        feature_properties: list[str],
        *,
        activation_function: Any | None = None,
        negative_sample_weight: int | None = None,
        embedding_dimension: int | None = None,
        tolerance: float | None = None,
        learning_rate: float | None = None,
        max_iterations: int | None = None,
        sample_sizes: list[int] | None = None,
        aggregator: Any | None = None,
        penalty_l2: float | None = None,
        search_depth: int | None = None,
        epochs: int | None = None,
        projected_feature_dimension: int | None = None,
        batch_sampling_ratio: float | None = None,
        store_model_to_disk: bool | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        batch_size: int | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> EstimationResult:
        """
        Estimates memory requirements and other statistics for training a GraphSage model.

        This method provides memory estimation for the GraphSage training algorithm without
        actually executing the training. It helps determine the computational requirements
        before running the actual training procedure.

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
        relationship_types : list[str] | None = None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None = None
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
        EstimationResult
            The estimation result containing memory requirements and other statistics
        """


class GraphSageTrainResult(BaseResult):
    model_info: dict[str, Any]
    configuration: dict[str, Any]
    train_millis: int
