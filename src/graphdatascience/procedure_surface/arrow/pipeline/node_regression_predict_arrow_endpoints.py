from __future__ import annotations

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.node_regression_predict_endpoints import (
    NodeRegressionPipelinePredictEndpoints,
    NodeRegressionPipelinePredictMutateResult,
)
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter


class NodeRegressionPredictArrowEndpoints(NodeRegressionPipelinePredictEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None,
        show_progress: bool = True,
    ) -> None:
        self._arrow_client = arrow_client
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client,
            write_back_client,
            show_progress=show_progress,
        )
        self._show_progress = show_progress

    def stream(
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
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            model_name=model_name,
            relationship_types=relationship_types,
            target_node_labels=target_node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )
        show_progress = self._show_progress and log_progress
        result_job_id = JobClient.run_job_and_wait(
            self._arrow_client,
            "v2/pipeline.nodeRegression.predict",
            config,
            show_progress=show_progress,
        )
        return JobClient.stream_results(self._arrow_client, G.name(), result_job_id)

    def mutate(
        self,
        G: GraphV2,
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
        config = self._node_property_endpoints.create_base_config(
            G,
            model_name=model_name,
            relationship_types=relationship_types,
            target_node_labels=target_node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )

        raw_result = self._node_property_endpoints.run_job_and_mutate(
            "v2/pipeline.nodeRegression.predict",
            config,
            mutate_property,
        )

        return NodeRegressionPipelinePredictMutateResult(**raw_result)
