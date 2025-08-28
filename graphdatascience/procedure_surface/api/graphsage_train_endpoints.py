from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.model.graphsage_model import GraphSageModelV2

from ...graph.graph_object import Graph


class GraphSageTrainEndpoints(ABC):
    """
    Abstract base class defining the API for the GraphSage algorithm.
    """

    @abstractmethod
    def train(
        self,
        G: Graph,
        model_name: str,
        feature_properties: List[str],
        *,
        activation_function: Optional[Any] = None,
        negative_sample_weight: Optional[int] = None,
        embedding_dimension: Optional[int] = None,
        tolerance: Optional[float] = None,
        learning_rate: Optional[float] = None,
        max_iterations: Optional[int] = None,
        sample_sizes: Optional[List[int]] = None,
        aggregator: Optional[Any] = None,
        penalty_l2: Optional[float] = None,
        search_depth: Optional[int] = None,
        epochs: Optional[int] = None,
        projected_feature_dimension: Optional[int] = None,
        batch_sampling_ratio: Optional[float] = None,
        store_model_to_disk: Optional[bool] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        batch_size: Optional[int] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
    ) -> tuple[GraphSageModelV2, GraphSageTrainResult]:
        """
        Trains a GraphSage model on the given graph.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        model_name : str
            Name under which the model will be stored
        feature_properties : List[str]
            The names of the node properties to use as input features
        activation_function : Optional[Any], default=None
            The activation function to apply after each layer
        negative_sample_weight : Optional[int], default=None
            Weight of negative samples in the loss function
        embedding_dimension : Optional[int], default=None
            The dimension of the generated embeddings
        tolerance : Optional[float], default=None
            Tolerance for early stopping based on loss improvement
        learning_rate : Optional[float], default=None
            Learning rate for the training optimization
        max_iterations : Optional[int], default=None
            Maximum number of training iterations
        sample_sizes : Optional[List[int]], default=None
            Number of neighbors to sample at each layer
        aggregator : Optional[Any], default=None
            The aggregator function for neighborhood aggregation
        penalty_l2 : Optional[float], default=None
            L2 regularization penalty
        search_depth : Optional[int], default=None
            Maximum search depth for neighbor sampling
        epochs : Optional[int], default=None
            Number of training epochs
        projected_feature_dimension : Optional[int], default=None
            Dimension to project input features to before training
        batch_sampling_ratio : Optional[float], default=None
            Ratio of nodes to sample for each training batch
        store_model_to_disk : Optional[bool], default=None
            Whether to persist the model to disk
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        username : Optional[str] = None
            The username to attribute the procedure run to
        log_progress : Optional[bool], default=None
            Whether to log progress
        sudo : Optional[bool], default=None
            Override memory estimation limits
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        batch_size : Optional[int], default=None
            Batch size for training
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        random_seed : Optional[Any], default=None
            Random seed for reproducible results

        Returns
        -------
        GraphSageModelV2
            Trained model
        """


class GraphSageTrainResult(BaseResult):
    model_info: dict[str, Any]
    configuration: dict[str, Any]
    train_millis: int
