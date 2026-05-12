from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class NodeClassificationPipelinePredictEndpoints(ABC):
    @abstractmethod
    def estimate(
        self,
        G: GraphV2,
        model_name: str,
        *,
        relationship_types: list[str] | None = None,
        target_node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory required to run node classification prediction.

        Parameters
        ----------
        G
            Graph object to use
        model_name
            Name of the model.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        target_node_labels
            Optional node label filter.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        log_progress
            Display progress logging.
        sudo
            Disable the memory guard.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

        Returns
        -------
        EstimationResult
            The estimated memory footprint for prediction.
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        model_name: str,
        *,
        relationship_types: list[str] | None = None,
        target_node_labels: list[str] | None = None,
        include_predicted_probabilities: bool = False,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Run node classification prediction in stream mode.

        Parameters
        ----------
        G
            Graph object to use
        model_name
            Name of the model.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        target_node_labels
            Optional node label filter.
        include_predicted_probabilities
            Whether to include the predicted probability distribution in the streamed results.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        log_progress
            Display progress logging.
        sudo
            Disable the memory guard.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

        Returns
        -------
        DataFrame
            The prediction results as a DataFrame.
        """
        pass

    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        model_name: str,
        mutate_property: str,
        *,
        relationship_types: list[str] | None = None,
        target_node_labels: list[str] | None = None,
        predicted_probability_property: str | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> NodeClassificationPipelinePredictMutateResult:
        """
        Run node classification prediction in mutate mode.

        Parameters
        ----------
        G
            Graph object to use
        model_name
            Name of the model.
        mutate_property
            Name of the node property to store the results in.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        target_node_labels
            Optional node label filter.
        predicted_probability_property
            Optional node property to store the predicted probability distribution in.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        log_progress
            Display progress logging.
        sudo
            Disable the memory guard.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

        Returns
        -------
        NodeClassificationPipelinePredictMutateResult
            The mutate result summary.
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        model_name: str,
        write_property: str,
        *,
        relationship_types: list[str] | None = None,
        target_node_labels: list[str] | None = None,
        predicted_probability_property: str | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        write_concurrency: int | None = None,
        job_id: str | None = None,
    ) -> NodeClassificationPipelinePredictWriteResult:
        """
        Run node classification prediction in write mode.

        Parameters
        ----------
        G
            Graph object to use
        model_name
            Name of the model.
        write_property
            Name of the node property to store the results in.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        target_node_labels
            Optional node label filter.
        predicted_probability_property
            Optional node property to store the predicted probability distribution in.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        log_progress
            Display progress logging.
        sudo
            Disable the memory guard.
        concurrency
            Number of concurrent threads to use.
        write_concurrency
            Number of concurrent threads to use for writing.
        job_id
            Identifier for the computation.

        Returns
        -------
        NodeClassificationPipelinePredictWriteResult
            The write result summary.
        """
        pass


class NodeClassificationPipelinePredictMutateResult(BaseResult):
    compute_millis: int | None = None
    configuration: dict[str, Any] | None = None
    mutate_millis: int | None = None
    node_properties_written: int | None = None
    post_processing_millis: int | None = None
    pre_processing_millis: int | None = None


class NodeClassificationPipelinePredictWriteResult(BaseResult):
    compute_millis: int | None = None
    configuration: dict[str, Any] | None = None
    node_properties_written: int | None = None
    post_processing_millis: int | None = None
    pre_processing_millis: int | None = None
    write_millis: int | None = None
