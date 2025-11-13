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
        activation_function: str = "SIGMOID",
        negative_sample_weight: int = 20,
        embedding_dimension: int = 64,
        tolerance: float = 0.0001,
        learning_rate: float = 0.1,
        max_iterations: int = 10,
        sample_sizes: list[int] | None = None,
        aggregator: str = "MEAN",
        penalty_l2: float = 0.0,
        search_depth: int = 5,
        epochs: int = 1,
        projected_feature_dimension: int | None = None,
        batch_sampling_ratio: float | None = None,
        store_model_to_disk: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
        batch_size: int = 100,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
    ) -> tuple[GraphSageModelV2, GraphSageTrainResult]: ...

    @abstractmethod
    def estimate(
        self,
        G: GraphV2,
        model_name: str,
        feature_properties: list[str],
        *,
        activation_function: str = "SIGMOID",
        negative_sample_weight: int = 20,
        embedding_dimension: int = 64,
        tolerance: float = 0.0001,
        learning_rate: float = 0.1,
        max_iterations: int = 10,
        sample_sizes: list[int] | None = None,
        aggregator: str = "MEAN",
        penalty_l2: float = 0.0,
        search_depth: int = 5,
        epochs: int = 1,
        projected_feature_dimension: int | None = None,
        batch_sampling_ratio: float | None = None,
        store_model_to_disk: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
        batch_size: int = 100,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
    ) -> EstimationResult:
        """
        Estimates memory requirements and other statistics for training a GraphSage model.

        This method provides memory estimation for the GraphSage training algorithm without
        actually executing the training. It helps determine the computational requirements
        before running the actual training procedure.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        model_name : str
            Name under which the model will be stored
        feature_properties : list[str]
            The names of the node properties to use as input features
        activation_function : str = "SIGMOID"
            The activation function to apply after each layer
        negative_sample_weight : int = 20
            Weight of negative samples in the loss function
        embedding_dimension : int = 64
            The dimension of the generated embeddings
        tolerance : float = 0.0001
            Tolerance for early stopping based on loss improvement
        learning_rate : float = 0.1
            Learning rate for the training optimization
        max_iterations
            Maximum number of iterations to run.
        sample_sizes : list[int] | None = None
            Number of neighbors to sample at each layer. Defaults to [25, 10] if not specified
        aggregator : str = "MEAN"
            The aggregator function for neighborhood aggregation
        penalty_l2 : float = 0.0
            L2 regularization penalty
        search_depth : int = 5
            Maximum search depth for neighbor sampling
        epochs : int = 1
            Number of training epochs
        projected_feature_dimension : int | None = None
            Dimension to project input features to before training
        batch_sampling_ratio : float | None = None
            Ratio of nodes to sample for each training batch
        store_model_to_disk : bool = False
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
        batch_size : int = 100
            Batch size for training
        relationship_weight_property
            Name of the property to be used as weights.
        random_seed
            Seed for random number generation to ensure reproducible results.

        Returns
        -------
        EstimationResult
            The estimation result containing memory requirements and other statistics
        """


class GraphSageTrainResult(BaseResult):
    model_info: dict[str, Any]
    configuration: dict[str, Any]
    train_millis: int
