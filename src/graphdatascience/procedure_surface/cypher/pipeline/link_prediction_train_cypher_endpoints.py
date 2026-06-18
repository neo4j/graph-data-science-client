from __future__ import annotations

from typing import Any

from graphdatascience.call_parameters import CallParameters
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
from graphdatascience.procedure_surface.cypher.model_api_cypher import ModelApiCypher
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class LinkPredictionTrainCypherEndpoints(LinkPredictionPipelineTrainEndpoints):
    def __init__(self, query_runner: QueryRunner, predict_endpoints: LinkPredictionPipelinePredictEndpoints):
        self._query_runner = query_runner
        self._predict_endpoints = predict_endpoints

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
            metrics=metrics,
            model_name=model_name,
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
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.linkPrediction.train",
            params=params,
            logging=True,
        ).squeeze()
        return (
            LinkPredictionModel(
                name=model_name,
                model_api=ModelApiCypher(self._query_runner),
                predict_endpoints=self._predict_endpoints,
            ),
            LinkPredictionPipelineTrainResult(**result.to_dict()),
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
        config = ConfigConverter.convert_to_gds_config(
            metrics=metrics,
            model_name=model_name,
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
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.linkPrediction.train.estimate",
            params=params,
        ).squeeze()
        return EstimationResult.from_cypher(result.to_dict())
