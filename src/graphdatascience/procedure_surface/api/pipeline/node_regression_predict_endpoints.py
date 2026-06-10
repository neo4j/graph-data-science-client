from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.base_result import BaseResult


class NodeRegressionPipelinePredictEndpoints(ABC):
    @abstractmethod
    def stream(
        self,
        G: Graph,
        model_name: str,
        *,
        relationship_types: list[str] | None = None,
        target_node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Run node regression prediction in stream mode.

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
        DataFrame
            The prediction results as a DataFrame.
        """
        pass

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        model_name: str,
        mutate_property: str,
        *,
        relationship_types: list[str] | None = None,
        target_node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> NodeRegressionPipelinePredictMutateResult:
        """
        Run node regression prediction in mutate mode.

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
        NodeRegressionPipelinePredictMutateResult
            The mutate result summary.
        """
        pass


class NodeRegressionPipelinePredictMutateResult(BaseResult):
    compute_millis: int | None = None
    configuration: dict[str, Any] | None = None
    mutate_millis: int | None = None
    node_properties_written: int | None = None
    post_processing_millis: int | None = None
    pre_processing_millis: int | None = None
