from typing import Any, Optional

from graphdatascience import Graph, QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.arrow_client.v2.write_back_client import WriteBackClient
from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.node_label_endpoints import (
    NodeLabelEndpoints,
    NodeLabelMutateResult,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter


class NodeLabelArrowEndpoints(NodeLabelEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[WriteBackClient] = None):
        self._arrow_client = arrow_client
        self._write_back_client = write_back_client

    def mutate(
        self,
        G: Graph,
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
            node_filter=node_filter,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            write_concurrency=write_concurrency,
            job_id=job_id,
        )

        job_id = JobClient.run_job_and_wait(self._arrow_client, "v2/graph.nodeLabel.mutate", config)
        return NodeLabelMutateResult(**JobClient.get_summary(self._arrow_client, job_id))
