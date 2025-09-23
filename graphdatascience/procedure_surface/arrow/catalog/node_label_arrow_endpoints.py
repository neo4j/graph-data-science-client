from typing import Any, Optional

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.node_label_endpoints import (
    NodeLabelEndpoints,
    NodeLabelMutateResult,
    NodeLabelWriteResult,
)
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter


class NodeLabelArrowEndpoints(NodeLabelEndpoints):
    def __init__(
        self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[RemoteWriteBackClient] = None
    ):
        self._arrow_client = arrow_client
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client, write_back_client)
        self._show_progress = False  # TODO add option to show progress

    def mutate(
        self,
        G: GraphV2,
        node_label: str,
        *,
        node_filter: str,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> NodeLabelMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            node_label=node_label,
            node_filter=node_filter,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            write_concurrency=write_concurrency,
            job_id=job_id,
        )

        show_progress = self._show_progress and log_progress if log_progress is not None else self._show_progress
        job_id = JobClient.run_job_and_wait(
            self._arrow_client, "v2/graph.nodeLabel.mutate", config, show_progress=show_progress
        )
        return NodeLabelMutateResult(**JobClient.get_summary(self._arrow_client, job_id))

    def write(
        self,
        G: GraphV2,
        node_label: str,
        *,
        node_filter: str,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> NodeLabelWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            node_label=node_label,
            node_filter=node_filter,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/graph.nodeLabel.stream", G, config, write_concurrency, concurrency
        )
        return NodeLabelWriteResult(**result)
