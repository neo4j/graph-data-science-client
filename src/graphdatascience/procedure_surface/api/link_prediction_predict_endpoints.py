from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class LinkPredictionPipelinePredictEndpoints(ABC):
    @abstractmethod
    def estimate(
        self,
        G: GraphV2,
        model_name: str,
        *,
        source_node_label: str | None = None,
        target_node_label: str | None = None,
        top_n: int | None = None,
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
        source_node_label: str | None = None,
        target_node_label: str | None = None,
        top_n: int | None = None,
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
        mutate_relationship_type: str,
        *,
        source_node_label: str | None = None,
        target_node_label: str | None = None,
        top_n: int | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> LinkPredictionPipelinePredictMutateResult:
        pass


class LinkPredictionPipelinePredictMutateResult(BaseResult):
    compute_millis: int | None = None
    configuration: dict[str, Any] | None = None
    mutate_millis: int | None = None
    post_processing_millis: int | None = None
    pre_processing_millis: int | None = None
    relationships_written: int | None = None
