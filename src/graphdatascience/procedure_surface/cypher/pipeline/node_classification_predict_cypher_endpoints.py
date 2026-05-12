from __future__ import annotations

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pipeline.node_classification_predict_endpoints import (
    NodeClassificationPipelinePredictEndpoints,
    NodeClassificationPipelinePredictMutateResult,
    NodeClassificationPipelinePredictWriteResult,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class NodeClassificationPredictCypherEndpoints(NodeClassificationPipelinePredictEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

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
        config = ConfigConverter.convert_to_gds_config(
            model_name=model_name,
            relationship_types=relationship_types,
            target_node_labels=target_node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.nodeClassification.predict.stream.estimate",
            params=params,
        ).squeeze()
        return EstimationResult.from_cypher(result.to_dict())

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
        config = ConfigConverter.convert_to_gds_config(
            model_name=model_name,
            relationship_types=relationship_types,
            target_node_labels=target_node_labels,
            include_predicted_probabilities=include_predicted_probabilities,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()
        return self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.nodeClassification.predict.stream", params=params, logging=log_progress
        )

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
        config = ConfigConverter.convert_to_gds_config(
            model_name=model_name,
            mutate_property=mutate_property,
            predicted_probability_property=predicted_probability_property,
            relationship_types=relationship_types,
            target_node_labels=target_node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.nodeClassification.predict.mutate", params=params
        ).squeeze()
        return NodeClassificationPipelinePredictMutateResult(**result.to_dict())

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
        config = ConfigConverter.convert_to_gds_config(
            model_name=model_name,
            write_property=write_property,
            predicted_probability_property=predicted_probability_property,
            relationship_types=relationship_types,
            target_node_labels=target_node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            write_concurrency=write_concurrency,
            job_id=job_id,
        )
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.nodeClassification.predict.write", params=params
        ).squeeze()
        return NodeClassificationPipelinePredictWriteResult(**result.to_dict())
