from __future__ import annotations

import logging
import time

from tenacity import retry, retry_if_result

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.job_not_finished_error import JobNotFinishedError
from graphdatascience.query_runner.progress.progress_bar import TqdmProgressBar
from graphdatascience.query_runner.protocol.write_protocols import JobStatus, WriteProtocol
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.retry_utils.retry_utils import before_log, job_wait_strategy


class WriteJobHandle:
    @staticmethod
    def create(
        write_protocol: WriteProtocol,
        graph_name: str,
        job_id: str,
        termination_flag: TerminationFlag,
        concurrency: int | None = None,
        property_overwrites: str | dict[str, str] | None = None,
        relationship_type_overwrite: str | None = None,
        log_progress: bool = True,
    ) -> WriteJobHandle:
        if isinstance(property_overwrites, str):
            # The remote write back procedure allows specifying a single overwrite. The key is ignored.
            property_overwrites = {property_overwrites: property_overwrites}

        write_protocol.start_job(
            graph_name, job_id, concurrency, property_overwrites, relationship_type_overwrite, log_progress
        )

        return WriteJobHandle(
            write_protocol,
            graph_name,
            job_id,
            time.time(),
            termination_flag,
        )

    def __init__(
        self,
        write_protocol: WriteProtocol,
        graph_name: str,
        job_id: str,
        started_at: float,
        termination_flag: TerminationFlag,
    ):
        self._write_protocol = write_protocol
        self._graph_name = graph_name
        self._job_id = job_id
        self._started_at = started_at
        self._terminal_status: JobStatus | None = None
        self._termination_flag = termination_flag

    def job_id(self) -> str:
        return self._job_id

    def status(self) -> JobStatus:
        if self._terminal_status is not None:
            return self._terminal_status
        status = self._write_protocol.get_status(self._job_id)
        if status.done:
            self._terminal_status = status
        return status

    def done(self) -> bool:
        return self.status().done

    def wait(self, log_progress: bool = True) -> None:
        if self._terminal_status is not None:
            return

        self._terminal_status = self._poll_until_done(log_progress)

    def result(self, *, wait: bool = True) -> WriteBackResult:
        if not self.done():
            if not wait:
                raise JobNotFinishedError(f"Write-back job '{self._job_id}' is not finished yet.")
            self.wait()

        status = self._terminal_status
        assert status is not None
        write_millis = int((time.time() - self._started_at) * 1000)
        return WriteBackResult(
            writtenNodeProperties=status.written_node_properties,
            writtenNodeLabels=status.written_node_labels,
            writtenRelationships=status.written_relationships,
            writeMillis=write_millis,
            status=status.status,
            progress=status.progress,
        )

    def _poll_until_done(self, log_progress: bool) -> JobStatus:
        """Block until the write-back job is done, optionally rendering a progress bar."""

        def is_not_done(status: JobStatus) -> bool:
            return not status.done

        logger = logging.getLogger()

        @retry(
            reraise=True,
            retry=retry_if_result(is_not_done),
            wait=job_wait_strategy(),
            before=before_log(
                f"Write-Back (graph: `{self._graph_name}`, jobId: `{self._job_id}`)",
                logger,
                logging.DEBUG,
            ),
        )
        def poll(progress_bar: TqdmProgressBar | None) -> JobStatus:
            self._termination_flag.assert_running()
            status = self._write_protocol.get_status(self._job_id)

            if progress_bar is not None:
                display_progress = 0.0 if status.progress < 0 else status.progress * 100
                progress_bar.update(status=status.status, progress=display_progress)

            return status

        if log_progress:
            with TqdmProgressBar(
                task_name=f"Write-Back (graph: {self._graph_name})",
                relative_progress=0.0,
            ) as progress_bar:
                final = poll(progress_bar)
        else:
            final = poll(None)

        return final


class WriteBackResult(BaseResult):
    written_node_properties: int
    written_node_labels: int
    written_relationships: int
    write_millis: int
    status: str
    progress: float
