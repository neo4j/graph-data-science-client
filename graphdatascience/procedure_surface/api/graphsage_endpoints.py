from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional

from pandas import DataFrame
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from graphdatascience.model.v2.graphsage_model import GraphSageModel

from ...graph.graph_object import Graph


class GraphSageEndpoints(ABC):
    """
    Abstract base class defining the API for the GraphSage algorithm.
    """

    @abstractmethod
    def train(
        self,
        G: Graph,
        model_name: str,
        feature_properties: List[str],
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
    ) -> GraphSageModel:
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
        GraphSageTrainResult
            Training metrics and model information
        """

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        model_name: str,
        mutate_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        batch_size: Optional[int] = None,
    ) -> GraphSageMutateResult:
        """
        Executes the GraphSage algorithm using a trained model and writes the results back to the graph as a node property.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        model_name : str
            Name of the trained GraphSage model to use
        mutate_property : str
            The name of the node property to store the embeddings
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
            Batch size for inference

        Returns
        -------
        GraphSageMutateResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def stream(
        self,
        G: Graph,
        model_name: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        batch_size: Optional[int] = None,
    ) -> DataFrame:
        """
        Executes the GraphSage algorithm using a trained model and returns the results as a stream.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        model_name : str
            Name of the trained GraphSage model to use
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
            Batch size for inference

        Returns
        -------
        DataFrame
            Embeddings as a stream with columns nodeId and embedding
        """

    @abstractmethod
    def write(
        self,
        G: Graph,
        model_name: str,
        write_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        batch_size: Optional[int] = None,
        write_concurrency: Optional[Any] = None,
    ) -> GraphSageWriteResult:
        """
        Executes the GraphSage algorithm using a trained model and writes the results back to the database.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        model_name : str
            Name of the trained GraphSage model to use
        write_property : str
            The name of the node property to write the embeddings to
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
            Batch size for inference
        write_concurrency : Optional[Any], default=None
            The number of concurrent threads used for writing result

        Returns
        -------
        GraphSageWriteResult
            Algorithm metrics and statistics
        """


class GraphSageTrainResult(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    model_info: dict[str, Any]
    configuration: dict[str, Any]
    train_millis: int

    def __getitem__(self, item: str) -> Any:
        return self.__dict__[item]


class GraphSageMutateResult(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    node_count: int
    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    configuration: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return self.__dict__[item]


class GraphSageWriteResult(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    node_count: int
    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    configuration: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return self.__dict__[item]
