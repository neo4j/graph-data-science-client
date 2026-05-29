from __future__ import annotations

import typing

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.query_runner.protocol.write_protocols import WriteProtocol


class JobInfo(BaseResult):
    job_id: str
    name: str


class JobNotFoundException(Exception):
    """Exception raised when a job with the specified ID is not found."""

    pass


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
            Graph object to use
        job_id
            Identifier of the job.
        """
        jobs = self.list()
        matching_jobs = [job for job in jobs if job.job_id == job_id]

        if not matching_jobs:
            raise JobNotFoundException(f"Job with id '{job_id}' not found")

        job = matching_jobs[0]

        return JobHandle(
            arrow_client=self._arrow_client,
            write_protocol=self._write_protocol,
            job_id=job_id,
            graph=G,
            show_progress=self._show_progress,
            endpoint=job.name,
        )

    def list(self) -> typing.List[JobInfo]:
        """
        List all known jobs on the server.

        Returns
        -------
        list[JobInfo]
            One row per job containing ``job_id`` and ``name``.
        """

        rows = deserialize(self._arrow_client.do_action_with_retry(self.LIST_ENDPOINT, {}))
        return [JobInfo(**row) for row in rows]
