from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class GraphSagePredictEndpoints(ABC):
    """
    Abstract base class defining the API for the GraphSage prediction algorithm.
    """

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        model_name: str,
        *,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
        batch_size: int = 100,
    ) -> DataFrame:
        """
        Uses a pre-trained GraphSage model to predict embeddings for a graph and returns the results as a stream.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        model_name : str
            Name under which the model will is stored
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        username : str | None = None
            The username to attribute the procedure run to
        log_progress : bool = True
            Whether to log progress
        sudo : bool = False
            Override memory estimation limits
        concurrency : Any | None, default=None
            The number of concurrent threads
        job_id : Any | None, default=None
            An identifier for the job
        batch_size : int = 100
            Batch size for training

        Returns
        -------
        DataFrame
            DataFrame with node IDs and their embeddings
        """

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        model_name: str,
        write_property: str,
        *,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        write_concurrency: int | None = None,
        job_id: str | None = None,
        batch_size: int = 100,
    ) -> GraphSageWriteResult:
        """
        Uses a pre-trained GraphSage model to predict embeddings for a graph and writes the results back to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        model_name : str
            Name under which the model will is stored
        write_property : str
            The name of the node property to write the embeddings to
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        username : str | None = None
            The username to attribute the procedure run to
        log_progress : bool = True
            Whether to log progress
        sudo : bool = False
            Override memory estimation limits
        concurrency : Any | None, default=None
            The number of concurrent threads
        write_concurrency : int | None, default=None
             The number of concurrent threads used for writing
        job_id : Any | None, default=None
            An identifier for the job
        batch_size : int = 100
            Batch size for training

        Returns
        -------
        GraphSageWriteResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        model_name: str,
        mutate_property: str,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
        batch_size: int = 100,
    ) -> GraphSageMutateResult:
        """
        Uses a pre-trained GraphSage model to predict embeddings for a graph and writes the results back to the graph as a node property.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        model_name : str
            Name under which the model will is stored
        mutate_property : str
            The name of the node property to store the embeddings
        relationship_types : list[str]
            The relationship types used to select relationships for this algorithm run
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        username : str | None = None
            The username to attribute the procedure run to
        log_progress : bool | None, default=None
            Whether to log progress
        sudo : bool
            Override memory estimation limits
        concurrency : Any | None, default=None
            The number of concurrent threads
        job_id : Any | None, default=None
            An identifier for the job
        batch_size : int | None, default=None
            Batch size for training

        Returns
        -------
        GraphSageMutateResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        model_name: str,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        batch_size: int = 100,
        concurrency: int | None = None,
        log_progress: bool = True,
        username: str | None = None,
        sudo: bool = False,
        job_id: str | None = None,
    ) -> EstimationResult:
        """
        Returns an estimation of the memory consumption for that procedure.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on or a dictionary representing the graph.
        model_name : str
            Name under which the model will is stored
        relationship_types : list[str]
            The relationship types to consider.
        node_labels : list[str]
            The node labels to consider.
        batch_size : int = 100
            The batch size for prediction.
        concurrency : int | None, default=None
            The concurrency for computation.
        log_progress : bool = True
            Whether to log progress.
        username : str | None, default=None
            The username for the operation.
        sudo : bool = False
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        job_id : str | None, default=None
            The job ID for the operation.

        Returns
        -------
        EstimationResult
            The estimated cost of running the algorithm
        """


class GraphSageMutateResult(BaseResult):
    node_count: int
    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    configuration: dict[str, Any]


class GraphSageWriteResult(BaseResult):
    node_count: int
    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    configuration: dict[str, Any]
