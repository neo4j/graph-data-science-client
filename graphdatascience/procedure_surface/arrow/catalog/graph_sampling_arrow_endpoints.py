from __future__ import annotations

from typing import Any, List, Optional

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.graph_sampling_endpoints import (
    GraphSamplingEndpoints,
    GraphSamplingResult,
    GraphWithSamplingResult,
)
from graphdatascience.procedure_surface.arrow.catalog.graph_backend_arrow import get_graph
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter


class GraphSamplingArrowEndpoints(GraphSamplingEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient, show_progress: bool = False):
        self._arrow_client = arrow_client
        self._show_progress = show_progress

    def rwr(
        self,
        G: GraphV2,
        graph_name: str,
        start_nodes: Optional[List[int]] = None,
        restart_probability: Optional[float] = None,
        sampling_ratio: Optional[float] = None,
        node_label_stratification: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> GraphWithSamplingResult:
        config = ConfigConverter.convert_to_gds_config(
            from_graph_name=G.name(),
            graph_name=graph_name,
            start_nodes=start_nodes,
            restart_probability=restart_probability,
            sampling_ratio=sampling_ratio,
            node_label_stratification=node_label_stratification,
            relationship_weight_property=relationship_weight_property,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        show_progress = self._show_progress and log_progress
        job_id = JobClient.run_job_and_wait(
            self._arrow_client, "v2/graph.sample.rwr", config, show_progress=show_progress
        )

        return GraphWithSamplingResult(
            get_graph(graph_name, self._arrow_client),
            GraphSamplingResult(**JobClient.get_summary(self._arrow_client, job_id)),
        )

    def cnarw(
        self,
        G: GraphV2,
        graph_name: str,
        start_nodes: Optional[List[int]] = None,
        restart_probability: Optional[float] = None,
        sampling_ratio: Optional[float] = None,
        node_label_stratification: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: bool = True,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> GraphWithSamplingResult:
        config = ConfigConverter.convert_to_gds_config(
            from_graph_name=G.name(),
            graph_name=graph_name,
            start_nodes=start_nodes,
            restart_probability=restart_probability,
            sampling_ratio=sampling_ratio,
            node_label_stratification=node_label_stratification,
            relationship_weight_property=relationship_weight_property,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        show_progress = self._show_progress and log_progress
        job_id = JobClient.run_job_and_wait(
            self._arrow_client, "v2/graph.sample.cnarw", config, show_progress=show_progress
        )

        return GraphWithSamplingResult(
            get_graph(graph_name, self._arrow_client),
            GraphSamplingResult(**JobClient.get_summary(self._arrow_client, job_id)),
        )
