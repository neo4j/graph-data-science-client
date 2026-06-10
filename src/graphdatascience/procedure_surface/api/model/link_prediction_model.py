from __future__ import annotations

from pandas import DataFrame

from graphdatascience.graph.graph_api import Graph
from graphdatascience.model.model import Model
from graphdatascience.model.model_api import ModelApi
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pipeline.link_prediction_predict_endpoints import (
    LinkPredictionPipelinePredictEndpoints,
    LinkPredictionPipelinePredictMutateResult,
)


class LinkPredictionModelV2(Model):
    """
    Represents a link prediction model in the model catalog.

    Construct this using: func:`gds.v2.pipeline.link_prediction.train()`.
    """

    def __init__(
        self, name: str, model_api: ModelApi, predict_endpoints: LinkPredictionPipelinePredictEndpoints
    ) -> None:
        super().__init__(name, model_api)
        self._predict_endpoints = predict_endpoints

    def predict_stream(
        self,
        G: Graph,
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
        return self._predict_endpoints.stream(
            G,
            model_name=self.name(),
            relationship_types=relationship_types,
            sample_rate=sample_rate,
            source_node_label=source_node_label,
            target_node_label=target_node_label,
            threshold=threshold,
            top_k=top_k,
            top_n=top_n,
            initial_sampler=initial_sampler,
            delta_threshold=delta_threshold,
            max_iterations=max_iterations,
            random_joins=random_joins,
            random_seed=random_seed,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )

    def predict_estimate(
        self,
        G: Graph,
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
        return self._predict_endpoints.estimate(
            G,
            model_name=self.name(),
            source_node_label=source_node_label,
            target_node_label=target_node_label,
            top_n=top_n,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )

    def predict_mutate(
        self,
        G: Graph,
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
        return self._predict_endpoints.mutate(
            G,
            model_name=self.name(),
            mutate_relationship_type=mutate_relationship_type,
            mutate_property=mutate_property,
            relationship_types=relationship_types,
            sample_rate=sample_rate,
            source_node_label=source_node_label,
            target_node_label=target_node_label,
            threshold=threshold,
            top_k=top_k,
            top_n=top_n,
            initial_sampler=initial_sampler,
            delta_threshold=delta_threshold,
            max_iterations=max_iterations,
            random_joins=random_joins,
            random_seed=random_seed,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )
