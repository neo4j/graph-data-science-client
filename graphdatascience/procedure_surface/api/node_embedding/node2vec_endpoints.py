from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
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
        iterations: int = 1,
        negative_sampling_rate: int = 5,
        positive_sampling_factor: float = 0.001,
        embedding_dimension: int = 128,
        embedding_initializer: str = "NORMALIZED",
        initial_learning_rate: float = 0.025,
        min_learning_rate: float = 0.0001,
        window_size: int = 10,
        negative_sampling_exponent: float = 0.75,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
    ) -> Node2VecMutateResult:
        """
        Executes the Node2Vec algorithm and writes the results back to the graph as a node property.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        mutate_property : str
            The name of the node property to store the embeddings
        iterations : int, default=1
            The number of training iterations
        negative_sampling_rate : int, default=5
            Number of negative samples for each positive sample
        positive_sampling_factor : float, default=0.001
            Factor to multiply positive sampling weights
        embedding_dimension : int, default=128
            The dimension of the generated embeddings
        embedding_initializer : str, default="NORMALIZED"
            Strategy for initializing node embeddings. Either "UNIFORM" or "NORMALIZED"
        initial_learning_rate : float, default=0.025
            The initial learning rate
        min_learning_rate : float, default=0.0001
            The minimum learning rate
        window_size : int, default=10
            Size of the context window
        negative_sampling_exponent : float, default=0.75
            Exponent for negative sampling probability distribution
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        username : str | None, default=None
            The username to attribute the procedure run to
        log_progress : bool, default=True
            Whether to log progress
        sudo
            Disable the memory guard.
        concurrency : int | None
            The number of concurrent threads
        job_id
            Identifier for the computation.
        walk_length : int, default=80
            The length of each random walk
        walks_per_node : int, default=10
            Number of walks to sample for each node
        in_out_factor : float, default=1.0
            Controls the likelihood of immediately revisiting a node in the walk
        return_factor : float, default=1.0
            Controls the likelihood of visiting already visited nodes
        walk_buffer_size : int, default=1000
            Buffer size for walk sampling
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        random_seed
            Seed for random number generation to ensure reproducible results.

        Returns
        -------
        Node2VecMutateResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        iterations: int = 1,
        negative_sampling_rate: int = 5,
        positive_sampling_factor: float = 0.001,
        embedding_dimension: int = 128,
        embedding_initializer: str = "NORMALIZED",
        initial_learning_rate: float = 0.025,
        min_learning_rate: float = 0.0001,
        window_size: int = 10,
        negative_sampling_exponent: float = 0.75,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
    ) -> DataFrame:
        """
        Executes the Node2Vec algorithm and returns the results as a stream.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        iterations : int, default=1
            The number of training iterations
        negative_sampling_rate : int, default=5
            Number of negative samples for each positive sample
        positive_sampling_factor : float, default=0.001
            Factor to multiply positive sampling weights
        embedding_dimension : int, default=128
            The dimension of the generated embeddings
        embedding_initializer : str, default="NORMALIZED"
            Strategy for initializing node embeddings. Either "UNIFORM" or "NORMALIZED"
        initial_learning_rate : float, default=0.025
            The initial learning rate
        min_learning_rate : float, default=0.0001
            The minimum learning rate
        window_size : int, default=10
            Size of the context window
        negative_sampling_exponent : float, default=0.75
            Exponent for negative sampling probability distribution
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        username : str | None, default=None
            The username to attribute the procedure run to
        log_progress : bool, default=True
            Whether to log progress
        sudo
            Disable the memory guard.
        concurrency : int | None
            The number of concurrent threads
        job_id
            Identifier for the computation.
        walk_length : int, default=80
            The length of each random walk
        walks_per_node : int, default=10
            Number of walks to sample for each node
        in_out_factor : float, default=1.0
            Controls the likelihood of immediately revisiting a node in the walk
        return_factor : float, default=1.0
            Controls the likelihood of visiting already visited nodes
        walk_buffer_size : int, default=1000
            Buffer size for walk sampling
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        random_seed
            Seed for random number generation to ensure reproducible results.

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
        iterations: int = 1,
        negative_sampling_rate: int = 5,
        positive_sampling_factor: float = 0.001,
        embedding_dimension: int = 128,
        embedding_initializer: str = "NORMALIZED",
        initial_learning_rate: float = 0.025,
        min_learning_rate: float = 0.0001,
        window_size: int = 10,
        negative_sampling_exponent: float = 0.75,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
        write_concurrency: int | None = None,
    ) -> Node2VecWriteResult:
        """
        Executes the Node2Vec algorithm and writes the results back to the database.

        Parameters
        ----------
        G
            The graph to run the algorithm on.
        write_property : str
            The name of the node property to write the embeddings to
        iterations : int, default=1
            The number of training iterations
        negative_sampling_rate : int, default=5
            Number of negative samples for each positive sample
        positive_sampling_factor : float, default=0.001
            Factor to multiply positive sampling weights
        embedding_dimension : int, default=128
            The dimension of the generated embeddings
        embedding_initializer : str, default="NORMALIZED"
            Strategy for initializing node embeddings. Either "UNIFORM" or "NORMALIZED"
        initial_learning_rate : float, default=0.025
            The initial learning rate
        min_learning_rate : float, default=0.0001
            The minimum learning rate
        window_size : int, default=10
            Size of the context window
        negative_sampling_exponent : float, default=0.75
            Exponent for negative sampling probability distribution
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        username : str | None, default=None
            The username to attribute the procedure run to
        log_progress : bool, default=True
            Whether to log progress
        sudo
            Disable the memory guard.
        concurrency : int | None
            The number of concurrent threads
        job_id
            Identifier for the computation.
        walk_length : int, default=80
            The length of each random walk
        walks_per_node : int, default=10
            Number of walks to sample for each node
        in_out_factor : float, default=1.0
            Controls the likelihood of immediately revisiting a node in the walk
        return_factor : float, default=1.0
            Controls the likelihood of visiting already visited nodes
        walk_buffer_size : int, default=1000
            Buffer size for walk sampling
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        random_seed
            Seed for random number generation to ensure reproducible results.
        write_concurrency : int | None, default=None
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
        iterations: int = 1,
        negative_sampling_rate: int = 5,
        positive_sampling_factor: float = 0.001,
        embedding_dimension: int = 128,
        embedding_initializer: str = "NORMALIZED",
        initial_learning_rate: float = 0.025,
        min_learning_rate: float = 0.0001,
        window_size: int = 10,
        negative_sampling_exponent: float = 0.75,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        walk_length: int = 80,
        walks_per_node: int = 10,
        in_out_factor: float = 1.0,
        return_factor: float = 1.0,
        walk_buffer_size: int = 1000,
        relationship_weight_property: str | None = None,
        random_seed: int | None = None,
    ) -> EstimationResult:
        """
        Returns an estimation of the memory consumption for that procedure.

        Parameters
        ----------
        G
            The graph to run the algorithm on or a dictionary representing the graph.
        iterations : int, default=1
            The number of training iterations
        negative_sampling_rate : int, default=5
            Number of negative samples for each positive sample
        positive_sampling_factor : float, default=0.001
            Factor to multiply positive sampling weights
        embedding_dimension : int, default=128
            The dimension of the generated embeddings
        embedding_initializer : str, default="NORMALIZED"
            Strategy for initializing node embeddings. Either "UNIFORM" or "NORMALIZED"
        initial_learning_rate : float, default=0.025
            The initial learning rate
        min_learning_rate : float, default=0.0001
            The minimum learning rate
        window_size : int, default=10
            Size of the context window
        negative_sampling_exponent : float, default=0.75
            Exponent for negative sampling probability distribution
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        concurrency : int | None
            The number of concurrent threads
        walk_length : int, default=80
            The length of each random walk
        walks_per_node : int, default=10
            Number of walks to sample for each node
        in_out_factor : float, default=1.0
            Controls the likelihood of immediately revisiting a node in the walk
        return_factor : float, default=1.0
            Controls the likelihood of visiting already visited nodes
        walk_buffer_size : int, default=1000
            Buffer size for walk sampling
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        random_seed
            Seed for random number generation to ensure reproducible results.

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
