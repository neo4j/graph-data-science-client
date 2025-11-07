from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class Node2VecEndpoints(ABC):
    """
    Abstract base class defining the API for the Node2Vec algorithm.
    """

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        iterations: int | None = None,
        negative_sampling_rate: int | None = None,
        positive_sampling_factor: float | None = None,
        embedding_dimension: int | None = None,
        embedding_initializer: Any | None = None,
        initial_learning_rate: float | None = None,
        min_learning_rate: float | None = None,
        window_size: int | None = None,
        negative_sampling_exponent: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        walk_length: int | None = None,
        walks_per_node: int | None = None,
        in_out_factor: float | None = None,
        return_factor: float | None = None,
        walk_buffer_size: int | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> Node2VecMutateResult:
        """
        Executes the Node2Vec algorithm and writes the results back to the graph as a node property.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The name of the node property to store the embeddings
        iterations : int | None, default=None
            The number of training iterations
        negative_sampling_rate : int | None, default=None
            Number of negative samples for each positive sample
        positive_sampling_factor : float | None, default=None
            Factor to multiply positive sampling weights
        embedding_dimension : int | None, default=None
            The dimension of the generated embeddings
        embedding_initializer : Any | None, default=None
            Strategy for initializing node embeddings
        initial_learning_rate : float | None, default=None
            The initial learning rate
        min_learning_rate : float | None, default=None
            The minimum learning rate
        window_size : int | None, default=None
            Size of the context window
        negative_sampling_exponent : float | None, default=None
            Exponent for negative sampling probability distribution
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
        walk_length : int | None, default=None
            The length of each random walk
        walks_per_node : int | None, default=None
            Number of walks to sample for each node
        in_out_factor : float | None, default=None
            Controls the likelihood of immediately revisiting a node in the walk
        return_factor : float | None, default=None
            Controls the likelihood of visiting already visited nodes
        walk_buffer_size : int | None, default=None
            Buffer size for walk sampling
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        random_seed : Any | None, default=None
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
        iterations: int | None = None,
        negative_sampling_rate: int | None = None,
        positive_sampling_factor: float | None = None,
        embedding_dimension: int | None = None,
        embedding_initializer: Any | None = None,
        initial_learning_rate: float | None = None,
        min_learning_rate: float | None = None,
        window_size: int | None = None,
        negative_sampling_exponent: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        walk_length: int | None = None,
        walks_per_node: int | None = None,
        in_out_factor: float | None = None,
        return_factor: float | None = None,
        walk_buffer_size: int | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> DataFrame:
        """
        Executes the Node2Vec algorithm and returns the results as a stream.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        iterations : int | None, default=None
            The number of training iterations
        negative_sampling_rate : int | None, default=None
            Number of negative samples for each positive sample
        positive_sampling_factor : float | None, default=None
            Factor to multiply positive sampling weights
        embedding_dimension : int | None, default=None
            The dimension of the generated embeddings
        embedding_initializer : Any | None, default=None
            Strategy for initializing node embeddings
        initial_learning_rate : float | None, default=None
            The initial learning rate
        min_learning_rate : float | None, default=None
            The minimum learning rate
        window_size : int | None, default=None
            Size of the context window
        negative_sampling_exponent : float | None, default=None
            Exponent for negative sampling probability distribution
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
        walk_length : int | None, default=None
            The length of each random walk
        walks_per_node : int | None, default=None
            Number of walks to sample for each node
        in_out_factor : float | None, default=None
            Controls the likelihood of immediately revisiting a node in the walk
        return_factor : float | None, default=None
            Controls the likelihood of visiting already visited nodes
        walk_buffer_size : int | None, default=None
            Buffer size for walk sampling
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        random_seed : Any | None, default=None
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
        iterations: int | None = None,
        negative_sampling_rate: int | None = None,
        positive_sampling_factor: float | None = None,
        embedding_dimension: int | None = None,
        embedding_initializer: Any | None = None,
        initial_learning_rate: float | None = None,
        min_learning_rate: float | None = None,
        window_size: int | None = None,
        negative_sampling_exponent: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        walk_length: int | None = None,
        walks_per_node: int | None = None,
        in_out_factor: float | None = None,
        return_factor: float | None = None,
        walk_buffer_size: int | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
        write_concurrency: Any | None = None,
    ) -> Node2VecWriteResult:
        """
        Executes the Node2Vec algorithm and writes the results back to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The name of the node property to write the embeddings to
        iterations : int | None, default=None
            The number of training iterations
        negative_sampling_rate : int | None, default=None
            Number of negative samples for each positive sample
        positive_sampling_factor : float | None, default=None
            Factor to multiply positive sampling weights
        embedding_dimension : int | None, default=None
            The dimension of the generated embeddings
        embedding_initializer : Any | None, default=None
            Strategy for initializing node embeddings
        initial_learning_rate : float | None, default=None
            The initial learning rate
        min_learning_rate : float | None, default=None
            The minimum learning rate
        window_size : int | None, default=None
            Size of the context window
        negative_sampling_exponent : float | None, default=None
            Exponent for negative sampling probability distribution
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
        walk_length : int | None, default=None
            The length of each random walk
        walks_per_node : int | None, default=None
            Number of walks to sample for each node
        in_out_factor : float | None, default=None
            Controls the likelihood of immediately revisiting a node in the walk
        return_factor : float | None, default=None
            Controls the likelihood of visiting already visited nodes
        walk_buffer_size : int | None, default=None
            Buffer size for walk sampling
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        random_seed : Any | None, default=None
            Random seed for reproducible results
        write_concurrency : Any | None, default=None
            The number of concurrent threads used for writing result

        Returns
        -------
        Node2VecWriteResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        iterations: int | None = None,
        negative_sampling_rate: int | None = None,
        positive_sampling_factor: float | None = None,
        embedding_dimension: int | None = None,
        embedding_initializer: Any | None = None,
        initial_learning_rate: float | None = None,
        min_learning_rate: float | None = None,
        window_size: int | None = None,
        negative_sampling_exponent: float | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
        walk_length: int | None = None,
        walks_per_node: int | None = None,
        in_out_factor: float | None = None,
        return_factor: float | None = None,
        walk_buffer_size: int | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> EstimationResult:
        """
        Returns an estimation of the memory consumption for that procedure.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on or a dictionary representing the graph.
        iterations : int | None, default=None
            The number of training iterations
        negative_sampling_rate : int | None, default=None
            Number of negative samples for each positive sample
        positive_sampling_factor : float | None, default=None
            Factor to multiply positive sampling weights
        embedding_dimension : int | None, default=None
            The dimension of the generated embeddings
        embedding_initializer : Any | None, default=None
            Strategy for initializing node embeddings
        initial_learning_rate : float | None, default=None
            The initial learning rate
        min_learning_rate : float | None, default=None
            The minimum learning rate
        window_size : int | None, default=None
            Size of the context window
        negative_sampling_exponent : float | None, default=None
            Exponent for negative sampling probability distribution
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        concurrency : Any | None, default=None
            The number of concurrent threads
        walk_length : int | None, default=None
            The length of each random walk
        walks_per_node : int | None, default=None
            Number of walks to sample for each node
        in_out_factor : float | None, default=None
            Controls the likelihood of immediately revisiting a node in the walk
        return_factor : float | None, default=None
            Controls the likelihood of visiting already visited nodes
        walk_buffer_size : int | None, default=None
            Buffer size for walk sampling
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        random_seed : Any | None, default=None
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
    post_processing_millis: int
    mutate_millis: int
    configuration: dict[str, Any]
    loss_per_iteration: list[float]


class Node2VecWriteResult(BaseResult):
    node_count: int
    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    configuration: dict[str, Any]
    loss_per_iteration: list[float]
