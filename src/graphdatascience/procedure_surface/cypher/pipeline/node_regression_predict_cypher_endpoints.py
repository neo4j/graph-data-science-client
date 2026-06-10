from __future__ import annotations

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.pipeline.node_regression_predict_endpoints import (
    NodeRegressionPipelinePredictEndpoints,
    NodeRegressionPipelinePredictMutateResult,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class NodeRegressionPredictCypherEndpoints(NodeRegressionPipelinePredictEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def stream(
        self,
        G: Graph,
        model_name: str,
        *,
        relationship_types: list[str] | None = None,
        target_node_labels: list[str] | None = None,
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
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()
        return self._query_runner.call_procedure(
            endpoint="gds.alpha.pipeline.nodeRegression.predict.stream", params=params, logging=log_progress
        )

    def mutate(
        self,
        G: Graph,
        model_name: str,
        mutate_property: str,
        *,
        relationship_types: list[str] | None = None,
        target_node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> NodeRegressionPipelinePredictMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            model_name=model_name,
            mutate_property=mutate_property,
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
            endpoint="gds.alpha.pipeline.nodeRegression.predict.mutate", params=params
        ).squeeze()
        return NodeRegressionPipelinePredictMutateResult(**result.to_dict())
