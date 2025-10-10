from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class FastRPEndpoints(ABC):
    """
    Abstract base class defining the API for the FastRP (Fast Random Projection) algorithm.

    FastRP is a node embedding algorithm that creates vector representations of nodes
    by combining random projections and iterative neighbor aggregation.
    """

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        embedding_dimension: int,
        iteration_weights: list[float] | None = None,
        normalization_strength: float | None = None,
        node_self_influence: float | None = None,
        property_ratio: float | None = None,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> FastRPMutateResult:
        """
        Executes the FastRP algorithm and writes the results back to the graph as a node property.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the FastRP embeddings for each node
        embedding_dimension : int
            The dimension of the generated embeddings
        iteration_weights : list[float] | None, default=None
            Weights for each iteration. Controls the influence of each iteration on the final embedding
        normalization_strength : float | None, default=None
            The normalization strength parameter controls how much the embedding is normalized
        node_self_influence : float | None, default=None
            The influence of the node's own features on its embedding
        property_ratio : float | None, default=None
            The ratio of node properties to use in the embedding
        feature_properties : list[str] | None, default=None
            List of node properties to use as features in the embedding
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
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        random_seed : Any | None, default=None
            Random seed for reproducible results

        Returns
        -------
        FastRPMutateResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        embedding_dimension: int,
        iteration_weights: list[float] | None = None,
        normalization_strength: float | None = None,
        node_self_influence: float | None = None,
        property_ratio: float | None = None,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> FastRPStatsResult:
        """
        Executes the FastRP algorithm and returns result statistics without writing the result to Neo4j.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        embedding_dimension : int
            The dimension of the generated embeddings
        iteration_weights : list[float] | None, default=None
            Weights for each iteration. Controls the influence of each iteration on the final embedding
        normalization_strength : float | None, default=None
            The normalization strength parameter controls how much the embedding is normalized
        node_self_influence : float | None, default=None
            The influence of the node's own features on its embedding
        property_ratio : float | None, default=None
            The ratio of node properties to use in the embedding
        feature_properties : list[str] | None, default=None
            List of node properties to use as features in the embedding
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        sudo : bool | None, default=None
            Override memory estimation limits
        log_progress : bool | None, default=None
            Whether to log progress
        username : str | None = None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            The number of concurrent threads
        job_id : Any | None, default=None
            An identifier for the job
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        random_seed : Any | None, default=None
            Random seed for reproducible results

        Returns
        -------
        FastRPStatsResult
            Algorithm statistics
        """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        embedding_dimension: int,
        iteration_weights: list[float] | None = None,
        normalization_strength: float | None = None,
        node_self_influence: float | None = None,
        property_ratio: float | None = None,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> DataFrame:
        """
        Executes the FastRP algorithm and returns the results as a stream.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        embedding_dimension : int
            The dimension of the generated embeddings
        iteration_weights : list[float] | None, default=None
            Weights for each iteration. Controls the influence of each iteration on the final embedding
        normalization_strength : float | None, default=None
            The normalization strength parameter controls how much the embedding is normalized
        node_self_influence : float | None, default=None
            The influence of the node's own features on its embedding
        property_ratio : float | None, default=None
            The ratio of node properties to use in the embedding
        feature_properties : list[str] | None, default=None
            List of node properties to use as features in the embedding
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
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        random_seed : Any | None, default=None
            Random seed for reproducible results

        Returns
        -------
        DataFrame
            DataFrame with node IDs and their FastRP embeddings
        """

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
        embedding_dimension: int,
        iteration_weights: list[float] | None = None,
        normalization_strength: float | None = None,
        node_self_influence: float | None = None,
        property_ratio: float | None = None,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
        write_concurrency: int | None = None,
    ) -> FastRPWriteResult:
        """
        Executes the FastRP algorithm and writes the results to Neo4j.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to write the FastRP embeddings for each node
        embedding_dimension : int
            The dimension of the generated embeddings
        iteration_weights : list[float] | None, default=None
            Weights for each iteration. Controls the influence of each iteration on the final embedding
        normalization_strength : float | None, default=None
            The normalization strength parameter controls how much the embedding is normalized
        node_self_influence : float | None, default=None
            The influence of the node's own features on its embedding
        property_ratio : float | None, default=None
            The ratio of node properties to use in the embedding
        feature_properties : list[str] | None, default=None
            List of node properties to use as features in the embedding
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
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        random_seed : Any | None, default=None
            Random seed for reproducible results
        write_concurrency : int | None, default=None
            The number of concurrent threads used for writing

        Returns
        -------
        FastRPWriteResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        embedding_dimension: int,
        iteration_weights: list[float] | None = None,
        normalization_strength: float | None = None,
        node_self_influence: float | None = None,
        property_ratio: float | None = None,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> EstimationResult:
        """
        Returns an estimation of the memory consumption for that procedure.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on or a dictionary representing the graph.
        embedding_dimension : int
            The dimension of the generated embeddings
        iteration_weights : list[float] | None, default=None
            Weights for each iteration. Controls the influence of each iteration on the final embedding
        normalization_strength : float | None, default=None
            The normalization strength parameter controls how much the embedding is normalized
        node_self_influence : float | None, default=None
            The influence of the node's own features on its embedding
        property_ratio : float | None, default=None
            The ratio of node properties to use in the embedding
        feature_properties : list[str] | None, default=None
            List of node properties to use as features in the embedding
        relationship_types : list[str] | None, default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str] | None, default=None
            The node labels used to select nodes for this algorithm run
        concurrency : Any | None, default=None
            The number of concurrent threads
        relationship_weight_property : str | None, default=None
            The property name that contains weight
        random_seed : Any | None, default=None
            Random seed for reproducible results

        Returns
        -------
        EstimationResult
            Memory estimation details
        """


class FastRPMutateResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    node_properties_written: int
    configuration: dict[str, Any]


class FastRPStatsResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    configuration: dict[str, Any]


class FastRPWriteResult(BaseResult):
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    node_properties_written: int
    configuration: dict[str, Any]
