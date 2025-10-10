from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class HashGNNEndpoints(ABC):
    """
    Abstract base class for HashGNN (Heterogeneous GraphV2 Neural Network) endpoints.
    """

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        iterations: int,
        embedding_density: int,
        mutate_property: str,
        output_dimension: int | None = None,
        neighbor_influence: float | None = None,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool | None = None,
        feature_properties: list[str] | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> HashGNNMutateResult:
        """
        Executes the HashGNN algorithm and writes the results back to the graph as a node property.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        iterations : int
            The number of iterations to run the algorithm
        embedding_density : int
            The density of the generated embeddings (number of bits per embedding)
        mutate_property : str
            The name of the node property to store the embeddings
        output_dimension : int | None, default=None
            The dimension of the output embeddings
        neighbor_influence : float | None, default=None
            The influence of neighboring nodes
        generate_features : dict[str, Any] | None, default=None
            Configuration for generating synthetic features from existing node properties
        binarize_features : dict[str, Any] | None, default=None
            Configuration for binarizing continuous features
        heterogeneous : bool | None, default=None
            Whether to use heterogeneous node processing for different node types
        feature_properties : list[str] | None, default=None
            The names of the node properties to use as input features
        random_seed : int | None, default=None
            Seed for random number generation to ensure reproducible results

        Returns
        -------
        HashGNNMutateResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        iterations: int,
        embedding_density: int,
        output_dimension: int | None = None,
        neighbor_influence: float | None = None,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool | None = None,
        feature_properties: list[str] | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> DataFrame:
        """
        Executes the HashGNN algorithm and returns the results as a stream.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        iterations : int
            The number of iterations to run the algorithm
        embedding_density : int
            The density of the generated embeddings (number of bits per embedding)
        output_dimension : int | None, default=None
            The dimension of the output embeddings
        neighbor_influence : float | None, default=None
            The influence of neighboring nodes
        generate_features : dict[str, Any] | None, default=None
            Configuration for generating synthetic features from existing node properties
        binarize_features : dict[str, Any] | None, default=None
            Configuration for binarizing continuous features
        heterogeneous : bool | None, default=None
            Whether to use heterogeneous node processing for different node types
        feature_properties : list[str] | None, default=None
            The names of the node properties to use as input features
        random_seed : int | None, default=None
            Seed for random number generation to ensure reproducible results

        Returns
        -------
        DataFrame
            DataFrame with node IDs and their embeddings
        """

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        iterations: int,
        embedding_density: int,
        write_property: str,
        output_dimension: int | None = None,
        neighbor_influence: float | None = None,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool | None = None,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        write_concurrency: int | None = None,
        random_seed: int | None = None,
    ) -> HashGNNWriteResult:
        """
        Executes the HashGNN algorithm and writes the results back to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        iterations : int
            The number of iterations to run the algorithm
        embedding_density : int
            The density of the generated embeddings (number of bits per embedding)
        write_property : str
            The name of the node property to write the embeddings to
        output_dimension : int | None, default=None
            The dimension of the output embeddings. If not specified, defaults to embedding_density / 64
        neighbor_influence : float | None, default=None
            The influence of neighboring nodes (0.0 to 1.0)
        generate_features : dict[str, Any] | None, default=None
            Configuration for generating synthetic features from existing node properties
        binarize_features : dict[str, Any] | None, default=None
            Configuration for binarizing continuous features
        heterogeneous : bool | None, default=None
            Whether to use heterogeneous node processing for different node types
        feature_properties : list[str] | None, default=None
            The names of the node properties to use as input features
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        sudo : bool | None, default=None
            Override memory estimation limits
        log_progress : bool | None, default=None
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            The number of concurrent threads
        job_id : Any | None, default=None
            An identifier for the job
        write_concurrency : int | None, default=None
            The number of concurrent threads used for writing
        random_seed : int | None, default=None
            Seed for random number generation to ensure reproducible results

        Returns
        -------
        HashGNNWriteResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        iterations: int,
        embedding_density: int,
        output_dimension: int | None = None,
        neighbor_influence: float | None = None,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool | None = None,
        feature_properties: list[str] | None = None,
        random_seed: int | None = None,
    ) -> EstimationResult:
        """
        Returns an estimation of the memory consumption for that procedure.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on or a dictionary representing the graph.
        iterations : int
            The number of iterations to run the algorithm
        embedding_density : int
            The density of the generated embeddings (number of bits per embedding)
        output_dimension : int | None, default=None
            The dimension of the output embeddings.
        neighbor_influence : float | None, default=None
            The influence of neighboring nodes.
        generate_features : dict[str, Any] | None, default=None
            Configuration for generating synthetic features from existing node properties
        binarize_features : dict[str, Any] | None, default=None
            Configuration for binarizing continuous features
        heterogeneous : bool | None, default=None
            Whether to use heterogeneous node processing for different node types
        feature_properties : list[str] | None, default=None
            The names of the node properties to use as input features
        random_seed : int | None, default=None
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
    configuration: dict[str, Any]


class HashGNNWriteResult(BaseResult):
    """
    Result object representing the results of running a HashGNN algorithm in write mode.
    """

    node_count: int
    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    configuration: dict[str, Any]
