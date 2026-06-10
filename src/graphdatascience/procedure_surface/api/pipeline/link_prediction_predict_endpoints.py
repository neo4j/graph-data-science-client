from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class LinkPredictionPipelinePredictEndpoints(ABC):
    @abstractmethod
    def estimate(
        self,
        G: Graph,
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
        G: Graph,
        model_name: str,
        *,
        relationship_types: list[str] | None = None,
        sample_rate: float = 1.0,
        source_node_label: str | None = None,
        target_node_label: str | None = None,
        threshold: float | None = None,
        top_k: int | None = None,
        top_n: int | None = None,
        initial_sampler: str | None = None,
        delta_threshold: float | None = None,
        max_iterations: int | None = None,
        random_joins: int | None = None,
        random_seed: int | None = None,
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
        G: Graph,
        model_name: str,
        mutate_relationship_type: str,
        *,
        mutate_property: str = "probability",
        relationship_types: list[str] | None = None,
        sample_rate: float = 1.0,
        source_node_label: str | None = None,
        target_node_label: str | None = None,
        threshold: float | None = None,
        top_k: int | None = None,
        top_n: int | None = None,
        initial_sampler: str | None = None,
        delta_threshold: float | None = None,
        max_iterations: int | None = None,
        random_joins: int | None = None,
        random_seed: int | None = None,
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
    probability_distribution: dict[str, Any] | None = None
    relationships_written: int | None = None
    sampling_stats: dict[str, Any] | None = None
