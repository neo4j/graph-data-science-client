from __future__ import annotations

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pipeline.link_prediction_predict_endpoints import (
    LinkPredictionPipelinePredictEndpoints,
    LinkPredictionPipelinePredictMutateResult,
)
from graphdatascience.procedure_surface.arrow.relationship_endpoints_helper import RelationshipEndpointsHelper
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.protocol.write_protocols import WriteProtocol


class LinkPredictionPredictArrowEndpoints(LinkPredictionPipelinePredictEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None,
        show_progress: bool = True,
    ) -> None:
        self._arrow_client = arrow_client
        self._relationship_endpoints = RelationshipEndpointsHelper(
            arrow_client,
            write_protocol,
            show_progress=show_progress,
        )
        self._show_progress = show_progress

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
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            model_name=model_name,
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
        config = self._relationship_endpoints.create_base_config(
            G,
            model_name=model_name,
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

        raw_result = self._relationship_endpoints.run_job_and_mutate(
            "v2/pipeline.linkPrediction.predict",
            config,
            mutate_property=mutate_property,
            mutate_relationship_type=mutate_relationship_type,
        )
        return LinkPredictionPipelinePredictMutateResult(**raw_result)
