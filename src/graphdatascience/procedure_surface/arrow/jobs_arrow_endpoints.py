from __future__ import annotations

import time
import typing

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.projection_job_handle import ProjectionJobHandle
from graphdatascience.procedure_surface.api.write_job_handle import WriteJobHandle
from graphdatascience.query_runner.protocol.write_protocols import WriteProtocol
from graphdatascience.query_runner.termination_flag import TerminationFlag


class JobInfo(BaseResult):
    job_id: str
    job_name: str


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

    def get(self, G: Graph, job_id: str) -> JobHandle | WriteJobHandle | ProjectionJobHandle:
        """
        Returns the appropriate job handle for an existing job.

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

        if self._is_projection(job.job_name, G.name()):
            return ProjectionJobHandle(self._arrow_client, G.name(), job_id, TerminationFlag.create())
        elif self._write_protocol is not None:
            # currently there is no good way of checking if a job is a write job.
            # We can only try to check the job status and fall back to a regular job handle if it fails.
            try:
                self._write_protocol.get_status(job.job_id)
                return WriteJobHandle(self._write_protocol, G.name(), job_id, time.time(), TerminationFlag.create())
            except Exception:
                pass

        return JobHandle(
            arrow_client=self._arrow_client,
            write_protocol=self._write_protocol,
            job_id=job_id,
            graph=G,
            show_progress=self._show_progress,
            endpoint=job.job_name,
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

    def _is_projection(self, job_name: str, graph_name: str) -> bool:
        projection_endpoints = [
            "v2/graph.project.fromTables",
            "v2/graph.project.fromTriplets",
            "v2/graph.project.filter",
            "v2/graph.sample.rwr",
            "v2/graph.sample.cnarw",
            "v2/graph.generate",
        ]

        return job_name in projection_endpoints or job_name == graph_name
