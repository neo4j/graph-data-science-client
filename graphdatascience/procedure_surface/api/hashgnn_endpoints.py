from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult

from ...graph.graph_object import Graph
from .estimation_result import EstimationResult


class HashGNNEndpoints(ABC):
    """
    Abstract base class for HashGNN (Heterogeneous Graph Neural Network) endpoints.
    """

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        iterations: int,
        embedding_density: int,
        mutate_property: str,
        output_dimension: Optional[int] = None,
        neighbor_influence: Optional[float] = None,
        generate_features: Optional[Dict[str, Any]] = None,
        binarize_features: Optional[Dict[str, Any]] = None,
        heterogeneous: Optional[bool] = None,
        feature_properties: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> HashGNNMutateResult:
        """
        Executes the HashGNN algorithm and writes the results back to the graph as a node property.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        iterations : int
            The number of iterations to run the algorithm
        embedding_density : int
            The density of the generated embeddings (number of bits per embedding)
        mutate_property : str
            The name of the node property to store the embeddings
        output_dimension : Optional[int], default=None
            The dimension of the output embeddings
        neighbor_influence : Optional[float], default=None
            The influence of neighboring nodes
        generate_features : Optional[Dict[str, Any]], default=None
            Configuration for generating synthetic features from existing node properties
        binarize_features : Optional[Dict[str, Any]], default=None
            Configuration for binarizing continuous features
        heterogeneous : Optional[bool], default=None
            Whether to use heterogeneous node processing for different node types
        feature_properties : Optional[List[str]], default=None
            The names of the node properties to use as input features
        random_seed : Optional[int], default=None
            Seed for random number generation to ensure reproducible results

        Returns
        -------
        HashGNNMutateResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def stream(
        self,
        G: Graph,
        iterations: int,
        embedding_density: int,
        output_dimension: Optional[int] = None,
        neighbor_influence: Optional[float] = None,
        generate_features: Optional[Dict[str, Any]] = None,
        binarize_features: Optional[Dict[str, Any]] = None,
        heterogeneous: Optional[bool] = None,
        feature_properties: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> DataFrame:
        """
        Executes the HashGNN algorithm and returns the results as a stream.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        iterations : int
            The number of iterations to run the algorithm
        embedding_density : int
            The density of the generated embeddings (number of bits per embedding)
        output_dimension : Optional[int], default=None
            The dimension of the output embeddings
        neighbor_influence : Optional[float], default=None
            The influence of neighboring nodes
        generate_features : Optional[Dict[str, Any]], default=None
            Configuration for generating synthetic features from existing node properties
        binarize_features : Optional[Dict[str, Any]], default=None
            Configuration for binarizing continuous features
        heterogeneous : Optional[bool], default=None
            Whether to use heterogeneous node processing for different node types
        feature_properties : Optional[List[str]], default=None
            The names of the node properties to use as input features
        random_seed : Optional[int], default=None
            Seed for random number generation to ensure reproducible results

        Returns
        -------
        DataFrame
            DataFrame with node IDs and their embeddings
        """

    @abstractmethod
    def write(
        self,
        G: Graph,
        iterations: int,
        embedding_density: int,
        write_property: str,
        output_dimension: Optional[int] = None,
        neighbor_influence: Optional[float] = None,
        generate_features: Optional[Dict[str, Any]] = None,
        binarize_features: Optional[Dict[str, Any]] = None,
        heterogeneous: Optional[bool] = None,
        feature_properties: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[int] = None,
        random_seed: Optional[int] = None,
    ) -> HashGNNWriteResult:
        """
        Executes the HashGNN algorithm and writes the results back to the database.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        iterations : int
            The number of iterations to run the algorithm
        embedding_density : int
            The density of the generated embeddings (number of bits per embedding)
        write_property : str
            The name of the node property to write the embeddings to
        output_dimension : Optional[int], default=None
            The dimension of the output embeddings. If not specified, defaults to embedding_density / 64
        neighbor_influence : Optional[float], default=None
            The influence of neighboring nodes (0.0 to 1.0)
        generate_features : Optional[Dict[str, Any]], default=None
            Configuration for generating synthetic features from existing node properties
        binarize_features : Optional[Dict[str, Any]], default=None
            Configuration for binarizing continuous features
        heterogeneous : Optional[bool], default=None
            Whether to use heterogeneous node processing for different node types
        feature_properties : Optional[List[str]], default=None
            The names of the node properties to use as input features
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        write_concurrency : Optional[int], default=None
            The number of concurrent threads used for writing
        random_seed : Optional[int], default=None
            Seed for random number generation to ensure reproducible results

        Returns
        -------
        HashGNNWriteResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def estimate(
        self,
        G: Union[Graph, dict[str, Any]],
        iterations: int,
        embedding_density: int,
        output_dimension: Optional[int] = None,
        neighbor_influence: Optional[float] = None,
        generate_features: Optional[Dict[str, Any]] = None,
        binarize_features: Optional[Dict[str, Any]] = None,
        heterogeneous: Optional[bool] = None,
        feature_properties: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
    ) -> EstimationResult:
        """
        Returns an estimation of the memory consumption for that procedure.

        Parameters
        ----------
        G : Union[Graph, dict[str, Any]]
            The graph to run the algorithm on or a dictionary representing the graph.
        iterations : int
            The number of iterations to run the algorithm
        embedding_density : int
            The density of the generated embeddings (number of bits per embedding)
        output_dimension : Optional[int], default=None
            The dimension of the output embeddings.
        neighbor_influence : Optional[float], default=None
            The influence of neighboring nodes.
        generate_features : Optional[Dict[str, Any]], default=None
            Configuration for generating synthetic features from existing node properties
        binarize_features : Optional[Dict[str, Any]], default=None
            Configuration for binarizing continuous features
        heterogeneous : Optional[bool], default=None
            Whether to use heterogeneous node processing for different node types
        feature_properties : Optional[List[str]], default=None
            The names of the node properties to use as input features
        random_seed : Optional[int], default=None
            Seed for random number generation to ensure reproducible results

        Returns
        -------
        EstimationResult
            The estimated cost of running the algorithm
        """


class HashGNNMutateResult(BaseResult):
    """
    Result object representing the results of running a HashGNN algorithm in mutate mode.
    """

    node_count: int
    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    configuration: Dict[str, Any]


class HashGNNWriteResult(BaseResult):
    """
    Result object representing the results of running a HashGNN algorithm in write mode.
    """

    node_count: int
    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    configuration: Dict[str, Any]
