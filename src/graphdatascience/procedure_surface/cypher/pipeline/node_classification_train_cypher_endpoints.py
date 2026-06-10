from __future__ import annotations

from typing import Any

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.model.node_classification_model import NodeClassificationModelV2
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline_results import (
    NodeClassificationPipelineTrainResult,
)
from graphdatascience.procedure_surface.api.pipeline.node_classification_predict_endpoints import (
    NodeClassificationPipelinePredictEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.node_classification_train_endpoints import (
    NodeClassificationPipelineTrainEndpoints,
)
from graphdatascience.procedure_surface.cypher.model_api_cypher import ModelApiCypher
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class NodeClassificationTrainCypherEndpoints(NodeClassificationPipelineTrainEndpoints):
    def __init__(self, query_runner: QueryRunner, predict_endpoints: NodeClassificationPipelinePredictEndpoints):
        self._query_runner = query_runner
        self._predict_endpoints = predict_endpoints

    def __call__(
        self,
        G: Graph,
        pipeline_name: str,
        *,
        metrics: list[str],
        model_name: str,
        target_property: str,
        relationship_types: list[str] = ALL_TYPES,
        target_node_labels: list[str] = ALL_LABELS,
        store_model_to_disk: bool = False,
        random_seed: Any | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> tuple[NodeClassificationModelV2, NodeClassificationPipelineTrainResult]:
        gds_config = ConfigConverter.convert_to_gds_config(
            metrics=metrics,
            model_name=model_name,
            pipeline=pipeline_name,
            target_property=target_property,
            relationship_types=relationship_types,
            target_node_labels=target_node_labels,
            store_model_to_disk=store_model_to_disk,
            random_seed=random_seed,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )
        params = CallParameters(graph_name=G.name(), config=gds_config)
        params.ensure_job_id_in_config()
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.nodeClassification.train", params=params, logging=True
        ).squeeze()
        return (
            NodeClassificationModelV2(
                name=model_name,
                model_api=ModelApiCypher(self._query_runner),
                predict_endpoints=self._predict_endpoints,
            ),
            NodeClassificationPipelineTrainResult(**result.to_dict()),
        )

    def estimate(
        self,
        G: Graph,
        pipeline_name: str,
        *,
        metrics: list[str],
        model_name: str,
        target_property: str,
        relationship_types: list[str] = ALL_TYPES,
        target_node_labels: list[str] = ALL_LABELS,
        store_model_to_disk: bool = False,
        random_seed: Any | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> EstimationResult:
        gds_config = ConfigConverter.convert_to_gds_config(
            metrics=metrics,
            model_name=model_name,
            pipeline=pipeline_name,
            target_property=target_property,
            relationship_types=relationship_types,
            target_node_labels=target_node_labels,
            store_model_to_disk=store_model_to_disk,
            random_seed=random_seed,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )
        params = CallParameters(graph_name=G.name(), config=gds_config)
        params.ensure_job_id_in_config()
        result = self._query_runner.call_procedure(
            endpoint="gds.beta.pipeline.nodeClassification.train.estimate",
            params=params,
        ).squeeze()
        return EstimationResult.from_cypher(result.to_dict())
