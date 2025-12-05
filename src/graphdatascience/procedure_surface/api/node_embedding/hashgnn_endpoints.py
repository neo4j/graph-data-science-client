from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class HashGNNEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        iterations: int,
        embedding_density: int,
        mutate_property: str,
        output_dimension: int | None = None,
        neighbor_influence: float = 1.0,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool = False,
        feature_properties: list[str] | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> HashGNNMutateResult:
        """
        Executes the HashGNN algorithm and writes the results back to the graph as a node property.

        Parameters
        ----------
        G
           Graph object to use
        iterations
            Number of iterations to run.
        embedding_density : int
            The density of the generated embeddings (number of bits per embedding)
        mutate_property
            Name of the node property to store the results in.
        output_dimension : int | None, default=None
            The dimension of the output embeddings
        neighbor_influence : float, default=1.0
            The influence of neighboring nodes
        generate_features : dict[str, Any] | None, default=None
            Configuration for generating synthetic features from existing node properties
        binarize_features : dict[str, Any] | None, default=None
            Configuration for binarizing continuous features
        heterogeneous : bool, default=False
            Whether to use heterogeneous node processing for different node types
        feature_properties : list[str] | None, default=None
            The names of the node properties to use as input features.
            Defaults to [] if not specified
        random_seed
            Seed for random number generation to ensure reproducible results.

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
        neighbor_influence: float = 1.0,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool = False,
        feature_properties: list[str] | None = None,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Executes the HashGNN algorithm and returns the results as a stream.

        Parameters
        ----------
        G
           Graph object to use
        iterations
            Number of iterations to run.
        embedding_density : int
            The density of the generated embeddings (number of bits per embedding)
        output_dimension : int | None, default=None
            The dimension of the output embeddings
        neighbor_influence : float, default=1.0
            The influence of neighboring nodes
        generate_features : dict[str, Any] | None, default=None
            Configuration for generating synthetic features from existing node properties
        binarize_features : dict[str, Any] | None, default=None
            Configuration for binarizing continuous features
        heterogeneous : bool, default=False
            Whether to use heterogeneous node processing for different node types
        feature_properties : list[str] | None, default=None
            The names of the node properties to use as input features.
            Defaults to [] if not specified
        random_seed
            Seed for random number generation to ensure reproducible results.

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
        neighbor_influence: float = 1.0,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool = False,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
        random_seed: int | None = None,
    ) -> HashGNNWriteResult:
        """
        Executes the HashGNN algorithm and writes the results back to the database.

        Parameters
        ----------
        G
           Graph object to use
        iterations
            Number of iterations to run.
        embedding_density : int
            The density of the generated embeddings (number of bits per embedding)
        write_property
            Name of the node property to store the results in.
        output_dimension : int | None, default=None
            The dimension of the output embeddings. If not specified, defaults to embedding_density / 64
        neighbor_influence : float, default=1.0
            The influence of neighboring nodes (0.0 to 1.0)
        generate_features : dict[str, Any] | None, default=None
            Configuration for generating synthetic features from existing node properties
        binarize_features : dict[str, Any] | None, default=None
            Configuration for binarizing continuous features
        heterogeneous : bool, default=False
            Whether to use heterogeneous node processing for different node types
        feature_properties : list[str] | None, default=None
            The names of the node properties to use as input features.
            Defaults to [] if not specified
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        write_concurrency
            Number of concurrent threads to use for writing.
        random_seed
            Seed for random number generation to ensure reproducible results.

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
        neighbor_influence: float = 1.0,
        generate_features: dict[str, Any] | None = None,
        binarize_features: dict[str, Any] | None = None,
        heterogeneous: bool = False,
        feature_properties: list[str] | None = None,
        random_seed: int | None = None,
    ) -> EstimationResult:
        """
        Returns an estimation of the memory consumption for that procedure.

        Parameters
        ----------
        G
           Graph object to use or a dictionary representing the graph dimensions.
        iterations
            Number of iterations to run.
        embedding_density : int
            The density of the generated embeddings (number of bits per embedding)
        output_dimension : int | None, default=None
            The dimension of the output embeddings.
        neighbor_influence : float, default=1.0
            The influence of neighboring nodes.
        generate_features : dict[str, Any] | None, default=None
            Configuration for generating synthetic features from existing node properties
        binarize_features : dict[str, Any] | None, default=None
            Configuration for binarizing continuous features
        heterogeneous : bool, default=False
            Whether to use heterogeneous node processing for different node types
        feature_properties : list[str] | None, default=None
            The names of the node properties to use as input features.
            Defaults to [] if not specified
        random_seed
            Seed for random number generation to ensure reproducible results.

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
