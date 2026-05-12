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
