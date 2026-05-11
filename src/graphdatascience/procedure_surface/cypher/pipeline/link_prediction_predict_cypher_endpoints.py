from __future__ import annotations

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.link_prediction_predict_endpoints import (
    LinkPredictionPipelinePredictEndpoints,
    LinkPredictionPipelinePredictMutateResult,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class LinkPredictionPredictCypherEndpoints(LinkPredictionPipelinePredictEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

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
        config = ConfigConverter.convert_to_gds_config(
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
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.linkPrediction.predict.stream.estimate",
            params=params,
        ).squeeze()
        return EstimationResult.from_cypher(result.to_dict())

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
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()
        return self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.linkPrediction.predict.stream",
            params=params,
            logging=log_progress,
        )

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
        config = ConfigConverter.convert_to_gds_config(
            model_name=model_name,
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
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.linkPrediction.predict.mutate",
            params=params,
        ).squeeze()
        return LinkPredictionPipelinePredictMutateResult(**result.to_dict())
