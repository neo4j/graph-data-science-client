from __future__ import annotations

from collections import OrderedDict
from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.api_types import JobStatus
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.graph import Graph
from graphdatascience.procedure_surface.api.job_not_finished_error import JobNotFinishedError
from graphdatascience.procedure_surface.api.write_job_handle import WriteJobHandle
from graphdatascience.procedure_surface.arrow.mutation_runner import MutationRunner
from graphdatascience.procedure_surface.arrow.stream_result_mapper import apply_stream_mapper
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol


class JobHandle:
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None,
        job_id: str,
        graph: Graph | str,
        show_progress: bool,
        endpoint: str,
    ):
        self._arrow_client = arrow_client
        self._job_id = job_id
        self._graph_name = graph.name() if isinstance(graph, Graph) else graph
        self._show_progress = show_progress
        self._is_done = False
        self._write_protocol = write_protocol
        self._endpoint = endpoint
        self._mutation_runner = MutationRunner(arrow_client)

    def job_id(self) -> str:
        return self._job_id

    def status(self) -> JobStatus:
        status = JobClient.get_job_status(self._arrow_client, self._job_id)
        if status.succeeded() or status.aborted():
            self._is_done = True
        return status

    def done(self) -> bool:
        if self._is_done:
            return True
        status = self.status()
        return status.succeeded() or status.aborted()

    def wait(self, *, termination_flag: TerminationFlag | None = None) -> None:
        if self._is_done:
            return
        JobClient().wait_for_job(
            self._arrow_client,
            self._job_id,
            show_progress=self._show_progress,
            termination_flag=termination_flag,
        )
        self._is_done = True

    def cancel(self) -> None:
        JobClient.cancel_job(self._arrow_client, self._job_id)

    def summary(
        self,
        *,
        wait: bool = True,
        termination_flag: TerminationFlag | None = None,
    ) -> dict[str, Any]:
        self._ensure_done(wait=wait, termination_flag=termination_flag)
        result = JobClient.get_summary(self._arrow_client, self._job_id)
        if nested_config := result.get("configuration", None):
            MutationRunner.drop_write_internals(nested_config)
        return result

    def stream(
        self,
        *,
        wait: bool = True,
        termination_flag: TerminationFlag | None = None,
    ) -> DataFrame:
        self._ensure_done(wait=wait, termination_flag=termination_flag)
        result = JobClient.stream_results(self._arrow_client, self._graph_name, self._job_id)
        result = apply_stream_mapper(self._endpoint, result)
        return result

    def mutate(
        self,
        *,
        mutate_property: str | None = None,
        mutate_relationship_type: str | None = None,
        mutate_property_overwrites: OrderedDict[str, str] | None = None,
        wait: bool = True,
        termination_flag: TerminationFlag | None = None,
    ) -> dict[str, Any]:
        self._ensure_done(wait=wait, termination_flag=termination_flag)
        return self._mutation_runner.run_mutation(
            self._job_id,
            mutate_property=mutate_property,
            mutate_relationship_type=mutate_relationship_type,
            mutate_property_overwrites=mutate_property_overwrites,
        )

    def write(
        self,
        *,
        write_properties: str | dict[str, str] | None = None,
        write_relationship_types: str | None = None,
        concurrency: int | None = None,
    ) -> WriteJobHandle:
        if self._write_protocol is None:
            raise ValueError("This session or job does not support write operations.")

        return WriteJobHandle.create(
            self._write_protocol,
            self._graph_name,
            self._job_id,
            TerminationFlag.create(),
            concurrency,
            write_properties,
            write_relationship_types,
            self._show_progress,
        )

    def _ensure_done(self, *, wait: bool, termination_flag: TerminationFlag | None) -> None:
        if self._is_done:
            return
        if wait:
            self.wait(termination_flag=termination_flag)
        elif not self.done():
            raise JobNotFinishedError(self._job_id)
