from __future__ import annotations

from typing import Any

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.model.link_prediction_model import LinkPredictionModel
from graphdatascience.procedure_surface.api.pipeline.link_prediction_pipeline_results import (
    LinkPredictionPipelineTrainResult,
)
from graphdatascience.procedure_surface.api.pipeline.link_prediction_predict_endpoints import (
    LinkPredictionPipelinePredictEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.link_prediction_train_endpoints import (
    LinkPredictionPipelineTrainEndpoints,
)
from graphdatascience.procedure_surface.arrow.model_api_arrow import ModelApiArrow
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter


class LinkPredictionTrainArrowEndpoints(LinkPredictionPipelineTrainEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        model_api: ModelApiArrow,
        predict_endpoints: LinkPredictionPipelinePredictEndpoints,
        show_progress: bool = True,
    ) -> None:
        self._arrow_client = arrow_client
        self._model_api = model_api
        self._predict_endpoints = predict_endpoints
        self._show_progress = show_progress
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client,
            write_protocol=None,
            show_progress=show_progress,
        )

    def __call__(
        self,
        G: Graph,
        pipeline_name: str,
        *,
        model_name: str,
        metrics: list[str] = ["AUCPR"],
        negative_class_weight: float = 1.0,
        source_node_label: str = "*",
        target_node_label: str = "*",
        target_relationship_type: str,
        store_model_to_disk: bool = False,
        random_seed: Any | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> tuple[LinkPredictionModel, LinkPredictionPipelineTrainResult]:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            model_name=model_name,
            metrics=metrics,
            negative_class_weight=negative_class_weight,
            pipeline=pipeline_name,
            source_node_label=source_node_label,
            target_node_label=target_node_label,
            target_relationship_type=target_relationship_type,
            store_model_to_disk=store_model_to_disk,
            random_seed=random_seed,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )
        show_progress = self._show_progress and log_progress
        result_job_id = JobClient.run_job_and_wait(
            self._arrow_client,
            "v2/pipeline.linkPrediction.train",
            config,
            show_progress=show_progress,
        )
        result = JobClient.get_summary(self._arrow_client, result_job_id)
        return (
            LinkPredictionModel(
                model_name,
                self._model_api,
                predict_endpoints=self._predict_endpoints,
            ),
            LinkPredictionPipelineTrainResult(**result),
        )

    def estimate(
        self,
        G: Graph,
        pipeline_name: str,
        *,
        model_name: str,
        metrics: list[str] = ["AUCPR"],
        negative_class_weight: float = 1.0,
        source_node_label: str = "*",
        target_node_label: str = "*",
        target_relationship_type: str,
        store_model_to_disk: bool = False,
        random_seed: Any | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> EstimationResult:
        algo_config = self._node_property_endpoints.create_estimate_config(
            model_name=model_name,
            metrics=metrics,
            negative_class_weight=negative_class_weight,
            pipeline=pipeline_name,
            source_node_label=source_node_label,
            target_node_label=target_node_label,
            target_relationship_type=target_relationship_type,
            store_model_to_disk=store_model_to_disk,
            random_seed=random_seed,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )
        return self._node_property_endpoints.estimate(
            estimate_endpoint="v2/pipeline.linkPrediction.train.estimate",
            G=G,
            algo_config=algo_config,
        )
