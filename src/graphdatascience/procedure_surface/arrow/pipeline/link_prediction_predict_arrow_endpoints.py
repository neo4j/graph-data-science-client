from __future__ import annotations

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.link_prediction_predict_endpoints import (
    LinkPredictionPipelinePredictEndpoints,
    LinkPredictionPipelinePredictMutateResult,
)
from graphdatascience.procedure_surface.arrow.relationship_endpoints_helper import RelationshipEndpointsHelper
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter


class LinkPredictionPredictArrowEndpoints(LinkPredictionPipelinePredictEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None,
        show_progress: bool = True,
    ) -> None:
        self._arrow_client = arrow_client
        self._relationship_endpoints = RelationshipEndpointsHelper(
            arrow_client,
            write_back_client,
            show_progress=show_progress,
        )
        self._show_progress = show_progress

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
        algo_config = self._relationship_endpoints.create_estimate_config(
            model_name=model_name,
            source_node_label=source_node_label,
            target_node_label=target_node_label,
            top_n=top_n,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )
        return self._relationship_endpoints.estimate("v2/pipeline.linkPrediction.predict.estimate", G, algo_config)

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
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            model_name=model_name,
            source_node_label=source_node_label,
            target_node_label=target_node_label,
            top_n=top_n,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )
        show_progress = self._show_progress and log_progress
        result_job_id = JobClient.run_job_and_wait(
            self._arrow_client,
            "v2/pipeline.linkPrediction.predict",
            config,
            show_progress=show_progress,
        )
        return JobClient.stream_results(self._arrow_client, G.name(), result_job_id)

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
        config = self._relationship_endpoints.create_base_config(
            G,
            model_name=model_name,
            source_node_label=source_node_label,
            target_node_label=target_node_label,
            top_n=top_n,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )

        raw_result = self._relationship_endpoints.run_job_and_mutate(
            "v2/pipeline.linkPrediction.predict",
            config,
            mutate_property="probability",
            mutate_relationship_type=mutate_relationship_type,
        )
        return LinkPredictionPipelinePredictMutateResult(**raw_result)
