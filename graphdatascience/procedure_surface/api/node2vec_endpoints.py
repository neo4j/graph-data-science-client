from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2

from .estimation_result import EstimationResult


class Node2VecEndpoints(ABC):
    """
    Abstract base class defining the API for the Node2Vec algorithm.
    """

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        iterations: Optional[int] = None,
        negative_sampling_rate: Optional[int] = None,
        positive_sampling_factor: Optional[float] = None,
        embedding_dimension: Optional[int] = None,
        embedding_initializer: Optional[Any] = None,
        initial_learning_rate: Optional[float] = None,
        min_learning_rate: Optional[float] = None,
        window_size: Optional[int] = None,
        negative_sampling_exponent: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        walk_length: Optional[int] = None,
        walks_per_node: Optional[int] = None,
        in_out_factor: Optional[float] = None,
        return_factor: Optional[float] = None,
        walk_buffer_size: Optional[int] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
    ) -> Node2VecMutateResult:
        """
        Executes the Node2Vec algorithm and writes the results back to the graph as a node property.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The name of the node property to store the embeddings
        iterations : Optional[int], default=None
            The number of training iterations
        negative_sampling_rate : Optional[int], default=None
            Number of negative samples for each positive sample
        positive_sampling_factor : Optional[float], default=None
            Factor to multiply positive sampling weights
        embedding_dimension : Optional[int], default=None
            The dimension of the generated embeddings
        embedding_initializer : Optional[Any], default=None
            Strategy for initializing node embeddings
        initial_learning_rate : Optional[float], default=None
            The initial learning rate
        min_learning_rate : Optional[float], default=None
            The minimum learning rate
        window_size : Optional[int], default=None
            Size of the context window
        negative_sampling_exponent : Optional[float], default=None
            Exponent for negative sampling probability distribution
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
        walk_length : Optional[int], default=None
            The length of each random walk
        walks_per_node : Optional[int], default=None
            Number of walks to sample for each node
        in_out_factor : Optional[float], default=None
            Controls the likelihood of immediately revisiting a node in the walk
        return_factor : Optional[float], default=None
            Controls the likelihood of visiting already visited nodes
        walk_buffer_size : Optional[int], default=None
            Buffer size for walk sampling
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        random_seed : Optional[Any], default=None
            Random seed for reproducible results

        Returns
        -------
        Node2VecMutateResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        iterations: Optional[int] = None,
        negative_sampling_rate: Optional[int] = None,
        positive_sampling_factor: Optional[float] = None,
        embedding_dimension: Optional[int] = None,
        embedding_initializer: Optional[Any] = None,
        initial_learning_rate: Optional[float] = None,
        min_learning_rate: Optional[float] = None,
        window_size: Optional[int] = None,
        negative_sampling_exponent: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        walk_length: Optional[int] = None,
        walks_per_node: Optional[int] = None,
        in_out_factor: Optional[float] = None,
        return_factor: Optional[float] = None,
        walk_buffer_size: Optional[int] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
    ) -> DataFrame:
        """
        Executes the Node2Vec algorithm and returns the results as a stream.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        iterations : Optional[int], default=None
            The number of training iterations
        negative_sampling_rate : Optional[int], default=None
            Number of negative samples for each positive sample
        positive_sampling_factor : Optional[float], default=None
            Factor to multiply positive sampling weights
        embedding_dimension : Optional[int], default=None
            The dimension of the generated embeddings
        embedding_initializer : Optional[Any], default=None
            Strategy for initializing node embeddings
        initial_learning_rate : Optional[float], default=None
            The initial learning rate
        min_learning_rate : Optional[float], default=None
            The minimum learning rate
        window_size : Optional[int], default=None
            Size of the context window
        negative_sampling_exponent : Optional[float], default=None
            Exponent for negative sampling probability distribution
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
        walk_length : Optional[int], default=None
            The length of each random walk
        walks_per_node : Optional[int], default=None
            Number of walks to sample for each node
        in_out_factor : Optional[float], default=None
            Controls the likelihood of immediately revisiting a node in the walk
        return_factor : Optional[float], default=None
            Controls the likelihood of visiting already visited nodes
        walk_buffer_size : Optional[int], default=None
            Buffer size for walk sampling
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        random_seed : Optional[Any], default=None
            Random seed for reproducible results

        Returns
        -------
        DataFrame
            Embeddings as a stream with columns nodeId and embedding
        """

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
        iterations: Optional[int] = None,
        negative_sampling_rate: Optional[int] = None,
        positive_sampling_factor: Optional[float] = None,
        embedding_dimension: Optional[int] = None,
        embedding_initializer: Optional[Any] = None,
        initial_learning_rate: Optional[float] = None,
        min_learning_rate: Optional[float] = None,
        window_size: Optional[int] = None,
        negative_sampling_exponent: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        walk_length: Optional[int] = None,
        walks_per_node: Optional[int] = None,
        in_out_factor: Optional[float] = None,
        return_factor: Optional[float] = None,
        walk_buffer_size: Optional[int] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
    ) -> Node2VecWriteResult:
        """
        Executes the Node2Vec algorithm and writes the results back to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The name of the node property to write the embeddings to
        iterations : Optional[int], default=None
            The number of training iterations
        negative_sampling_rate : Optional[int], default=None
            Number of negative samples for each positive sample
        positive_sampling_factor : Optional[float], default=None
            Factor to multiply positive sampling weights
        embedding_dimension : Optional[int], default=None
            The dimension of the generated embeddings
        embedding_initializer : Optional[Any], default=None
            Strategy for initializing node embeddings
        initial_learning_rate : Optional[float], default=None
            The initial learning rate
        min_learning_rate : Optional[float], default=None
            The minimum learning rate
        window_size : Optional[int], default=None
            Size of the context window
        negative_sampling_exponent : Optional[float], default=None
            Exponent for negative sampling probability distribution
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
        walk_length : Optional[int], default=None
            The length of each random walk
        walks_per_node : Optional[int], default=None
            Number of walks to sample for each node
        in_out_factor : Optional[float], default=None
            Controls the likelihood of immediately revisiting a node in the walk
        return_factor : Optional[float], default=None
            Controls the likelihood of visiting already visited nodes
        walk_buffer_size : Optional[int], default=None
            Buffer size for walk sampling
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        random_seed : Optional[Any], default=None
            Random seed for reproducible results
        write_concurrency : Optional[Any], default=None
            The number of concurrent threads used for writing result

        Returns
        -------
        Node2VecWriteResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        iterations: Optional[int] = None,
        negative_sampling_rate: Optional[int] = None,
        positive_sampling_factor: Optional[float] = None,
        embedding_dimension: Optional[int] = None,
        embedding_initializer: Optional[Any] = None,
        initial_learning_rate: Optional[float] = None,
        min_learning_rate: Optional[float] = None,
        window_size: Optional[int] = None,
        negative_sampling_exponent: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        walk_length: Optional[int] = None,
        walks_per_node: Optional[int] = None,
        in_out_factor: Optional[float] = None,
        return_factor: Optional[float] = None,
        walk_buffer_size: Optional[int] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
    ) -> EstimationResult:
        """
        Returns an estimation of the memory consumption for that procedure.

        Parameters
        ----------
        G : Union[GraphV2, dict[str, Any]]
            The graph to run the algorithm on or a dictionary representing the graph.
        iterations : Optional[int], default=None
            The number of training iterations
        negative_sampling_rate : Optional[int], default=None
            Number of negative samples for each positive sample
        positive_sampling_factor : Optional[float], default=None
            Factor to multiply positive sampling weights
        embedding_dimension : Optional[int], default=None
            The dimension of the generated embeddings
        embedding_initializer : Optional[Any], default=None
            Strategy for initializing node embeddings
        initial_learning_rate : Optional[float], default=None
            The initial learning rate
        min_learning_rate : Optional[float], default=None
            The minimum learning rate
        window_size : Optional[int], default=None
            Size of the context window
        negative_sampling_exponent : Optional[float], default=None
            Exponent for negative sampling probability distribution
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        walk_length : Optional[int], default=None
            The length of each random walk
        walks_per_node : Optional[int], default=None
            Number of walks to sample for each node
        in_out_factor : Optional[float], default=None
            Controls the likelihood of immediately revisiting a node in the walk
        return_factor : Optional[float], default=None
            Controls the likelihood of visiting already visited nodes
        walk_buffer_size : Optional[int], default=None
            Buffer size for walk sampling
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        random_seed : Optional[Any], default=None
            Random seed for reproducible results

        Returns
        -------
        EstimationResult
            Memory estimation details
        """


class Node2VecMutateResult(BaseResult):
    node_count: int
    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    configuration: dict[str, Any]
    loss_per_iteration: List[float]


class Node2VecWriteResult(BaseResult):
    node_count: int
    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    configuration: dict[str, Any]
    loss_per_iteration: List[float]
