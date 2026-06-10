from __future__ import annotations

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pipeline.link_prediction_predict_endpoints import (
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
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()
        return self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.linkPrediction.predict.stream",
            params=params,
            logging=log_progress,
        )

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
        config = ConfigConverter.convert_to_gds_config(
            model_name=model_name,
            mutate_property=mutate_property,
            mutate_relationship_type=mutate_relationship_type,
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
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.linkPrediction.predict.mutate",
            params=params,
        ).squeeze()
        return LinkPredictionPipelinePredictMutateResult(**result.to_dict())
