from __future__ import annotations

from pandas import DataFrame

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.model.v2.model import Model
from graphdatascience.model.v2.model_api import ModelApi
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.link_prediction_predict_endpoints import (
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
        G: GraphV2,
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
        return self._predict_endpoints.stream(
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

    def predict_estimate(
        self,
        G: GraphV2,
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
        G: GraphV2,
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
        return self._predict_endpoints.mutate(
            G,
            model_name=self.name(),
            mutate_relationship_type=mutate_relationship_type,
            source_node_label=source_node_label,
            target_node_label=target_node_label,
            top_n=top_n,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )
