from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

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
        iteration_weights: Optional[List[float]] = None,
        normalization_strength: Optional[float] = None,
        node_self_influence: Optional[float] = None,
        property_ratio: Optional[float] = None,
        feature_properties: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
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
        iteration_weights : Optional[List[float]], default=None
            Weights for each iteration. Controls the influence of each iteration on the final embedding
        normalization_strength : Optional[float], default=None
            The normalization strength parameter controls how much the embedding is normalized
        node_self_influence : Optional[float], default=None
            The influence of the node's own features on its embedding
        property_ratio : Optional[float], default=None
            The ratio of node properties to use in the embedding
        feature_properties : Optional[List[str]], default=None
            List of node properties to use as features in the embedding
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
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        random_seed : Optional[Any], default=None
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
        iteration_weights: Optional[List[float]] = None,
        normalization_strength: Optional[float] = None,
        node_self_influence: Optional[float] = None,
        property_ratio: Optional[float] = None,
        feature_properties: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
    ) -> FastRPStatsResult:
        """
        Executes the FastRP algorithm and returns result statistics without writing the result to Neo4j.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        embedding_dimension : int
            The dimension of the generated embeddings
        iteration_weights : Optional[List[float]], default=None
            Weights for each iteration. Controls the influence of each iteration on the final embedding
        normalization_strength : Optional[float], default=None
            The normalization strength parameter controls how much the embedding is normalized
        node_self_influence : Optional[float], default=None
            The influence of the node's own features on its embedding
        property_ratio : Optional[float], default=None
            The ratio of node properties to use in the embedding
        feature_properties : Optional[List[str]], default=None
            List of node properties to use as features in the embedding
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str] = None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        random_seed : Optional[Any], default=None
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
        iteration_weights: Optional[List[float]] = None,
        normalization_strength: Optional[float] = None,
        node_self_influence: Optional[float] = None,
        property_ratio: Optional[float] = None,
        feature_properties: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
    ) -> DataFrame:
        """
        Executes the FastRP algorithm and returns the results as a stream.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        embedding_dimension : int
            The dimension of the generated embeddings
        iteration_weights : Optional[List[float]], default=None
            Weights for each iteration. Controls the influence of each iteration on the final embedding
        normalization_strength : Optional[float], default=None
            The normalization strength parameter controls how much the embedding is normalized
        node_self_influence : Optional[float], default=None
            The influence of the node's own features on its embedding
        property_ratio : Optional[float], default=None
            The ratio of node properties to use in the embedding
        feature_properties : Optional[List[str]], default=None
            List of node properties to use as features in the embedding
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
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        random_seed : Optional[Any], default=None
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
        iteration_weights: Optional[List[float]] = None,
        normalization_strength: Optional[float] = None,
        node_self_influence: Optional[float] = None,
        property_ratio: Optional[float] = None,
        feature_properties: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
        write_concurrency: Optional[int] = None,
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
        iteration_weights : Optional[List[float]], default=None
            Weights for each iteration. Controls the influence of each iteration on the final embedding
        normalization_strength : Optional[float], default=None
            The normalization strength parameter controls how much the embedding is normalized
        node_self_influence : Optional[float], default=None
            The influence of the node's own features on its embedding
        property_ratio : Optional[float], default=None
            The ratio of node properties to use in the embedding
        feature_properties : Optional[List[str]], default=None
            List of node properties to use as features in the embedding
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
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        random_seed : Optional[Any], default=None
            Random seed for reproducible results
        write_concurrency : Optional[int], default=None
            The number of concurrent threads used for writing

        Returns
        -------
        FastRPWriteResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        embedding_dimension: int,
        iteration_weights: Optional[List[float]] = None,
        normalization_strength: Optional[float] = None,
        node_self_influence: Optional[float] = None,
        property_ratio: Optional[float] = None,
        feature_properties: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
    ) -> EstimationResult:
        """
        Returns an estimation of the memory consumption for that procedure.

        Parameters
        ----------
        G : Union[GraphV2, dict[str, Any]]
            The graph to run the algorithm on or a dictionary representing the graph.
        embedding_dimension : int
            The dimension of the generated embeddings
        iteration_weights : Optional[List[float]], default=None
            Weights for each iteration. Controls the influence of each iteration on the final embedding
        normalization_strength : Optional[float], default=None
            The normalization strength parameter controls how much the embedding is normalized
        node_self_influence : Optional[float], default=None
            The influence of the node's own features on its embedding
        property_ratio : Optional[float], default=None
            The ratio of node properties to use in the embedding
        feature_properties : Optional[List[str]], default=None
            List of node properties to use as features in the embedding
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        random_seed : Optional[Any], default=None
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
