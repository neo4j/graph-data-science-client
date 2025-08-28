from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult

from ...graph.graph_object import Graph


class GraphSagePredictEndpoints(ABC):
    """
    Abstract base class defining the API for the GraphSage prediction algorithm.
    """

    @abstractmethod
    def stream(
        self,
        G: Graph,
        model_name: str,
        *,
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
        Uses a pre-trained GraphSage model to predict embeddings for a graph and returns the results as a stream.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        model_name : str
            Name under which the model will is stored
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

        Returns
        -------
        DataFrame
            DataFrame with node IDs and their embeddings
        """

    @abstractmethod
    def write(
        self,
        G: Graph,
        model_name: str,
        write_property: str,
        *,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        username: Optional[str] = None,
        log_progress: Optional[bool] = None,
        sudo: Optional[bool] = None,
        concurrency: Optional[Any] = None,
        write_concurrency: Optional[int] = None,
        job_id: Optional[Any] = None,
        batch_size: Optional[int] = None,
    ) -> GraphSageWriteResult:
        """ "
        Uses a pre-trained GraphSage model to predict embeddings for a graph and writes the results back to the database.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        model_name : str
            Name under which the model will is stored
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
        write_concurrency : Optional[int], default=None
             The number of concurrent threads used for writing
        job_id : Optional[Any], default=None
            An identifier for the job
        batch_size : Optional[int], default=None
            Batch size for training

        Returns
        -------
        GraphSageWriteResult
            Algorithm metrics and statistics
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
        """ "
        Uses a pre-trained GraphSage model to predict embeddings for a graph and writes the results back to the graph as a node property.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        model_name : str
            Name under which the model will is stored
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
            Batch size for training

        Returns
        -------
        GraphSageMutateResult
            Algorithm metrics and statistics
        """

    @abstractmethod
    def estimate(
        self,
        G: Union[Graph, dict[str, Any]],
        model_name: str,
        relationship_types: Optional[list[str]] = None,
        node_labels: Optional[list[str]] = None,
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        sudo: Optional[bool] = None,
        job_id: Optional[str] = None,
    ) -> EstimationResult:
        """
        Returns an estimation of the memory consumption for that procedure.

        Parameters
        ----------
        G : Union[Graph, dict[str, Any]]
            The graph to run the algorithm on or a dictionary representing the graph.
        model_name : str
            Name under which the model will is stored
        relationship_types : Optional[list[str]], default=None
            The relationship types to consider.
        node_labels : Optional[list[str]], default=None
            The node labels to consider.
        batch_size : Optional[int], default=None
            The batch size for prediction.
        concurrency : Optional[int], default=None
            The concurrency for computation.
        log_progress : Optional[bool], default=None
            Whether to log progress.
        username : Optional[str], default=None
            The username for the operation.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        job_id : Optional[str], default=None
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
