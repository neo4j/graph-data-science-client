from __future__ import annotations

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.query_runner.protocol.write_protocols import WriteProtocol


class JobsArrowEndpoints:
    """Endpoints for inspecting and controlling jobs on the GDS Arrow server."""

    LIST_ENDPOINT = "v2/jobs.list"

    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = False,
    ):
        self._arrow_client = arrow_client
        self._write_protocol = write_protocol
        self._show_progress = show_progress

    def get(self, G: GraphV2, job_id: str) -> JobHandle:
        """
        Return a :class:`JobHandle` for an existing job.

        Parameters
        ----------
        G
            Graph that the job is running on.
        job_id
            Identifier of the job.
        """

        return JobHandle(
            arrow_client=self._arrow_client,
            write_protocol=self._write_protocol,
            job_id=job_id,
            graph=G,
            show_progress=self._show_progress,
        )

    def list(self) -> list[JobInfo]:
        """
        List all known jobs on the server.

        Returns
        -------
        list[JobInfo]
            One row per job containing ``job_id`` and ``name``.
        """

        rows = deserialize(self._arrow_client.do_action_with_retry(self.LIST_ENDPOINT, {}))
        return [JobInfo(**row) for row in rows]


class JobInfo(BaseResult):
    job_id: str
    name: str
