from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from pandas import DataFrame, Series
from tenacity import retry, retry_if_result

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.call_parameters import CallParameters
from graphdatascience.query_runner.progress.progress_bar import TqdmProgressBar
from graphdatascience.query_runner.protocol.arrow_config import build_arrow_config
from graphdatascience.query_runner.protocol.status import Status
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.retry_utils.retry_utils import before_log, job_wait_strategy
from graphdatascience.session.dbms.protocol_version import ProtocolVersion


@dataclass(frozen=True)
class JobStatus:
    """Protocol-agnostic snapshot of a write-back job's state."""

    done: bool
    raw_status: str
    progress: float
    error: str | None
    result_row: dict[str, Any]


class WriteProtocol(ABC):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        query_runner: QueryRunner,
        termination_flag: TerminationFlag,
        progress_bar_options: dict[str, Any] | None = None,
    ):
        self._arrow_client = arrow_client
        self._query_runner = query_runner
        self._termination_flag = termination_flag
        self._progress_bar_options = progress_bar_options or {}

    def run_write_back(
        self,
        graph_name: str,
        job_id: str,
        concurrency: int | None = None,
        property_overwrites: dict[str, str] | None = None,
        relationship_type_overwrite: str | None = None,
        log_progress: bool = True,
    ) -> DataFrame:
        parameters = self._build_call_parameters(
            graph_name, job_id, concurrency, property_overwrites, relationship_type_overwrite
        )

        self._start_job(parameters)

        def is_not_done(status: JobStatus) -> bool:
            return not status.done

        logger = logging.getLogger()

        @retry(
            reraise=True,
            retry=retry_if_result(is_not_done),
            wait=job_wait_strategy(),
            before=before_log(
                f"Write-Back (graph: `{graph_name}`, jobId: `{job_id}`)",
                logger,
                logging.DEBUG,
            ),
        )
        def poll(progress_bar: TqdmProgressBar | None) -> JobStatus:
            self._termination_flag.assert_running()
            status = self._get_status(job_id, parameters)

            if status.error is not None:
                raise Exception(status.error)

            if progress_bar is not None:
                display_progress = 0.0 if status.progress < 0 else status.progress * 100
                progress_bar.update(status=status.raw_status, progress=display_progress)

            return status

        if log_progress:
            with TqdmProgressBar(
                task_name=f"Write-Back (graph: {graph_name})",
                relative_progress=0.0,
                bar_options=self._progress_bar_options,
            ) as progress_bar:
                final = poll(progress_bar)
        else:
            final = poll(None)

        return DataFrame([final.result_row])

    @abstractmethod
    def _start_job(self, parameters: CallParameters) -> None:
        """Optional initial call to start the write-back job. No-op for protocols that combine start+poll."""

    @abstractmethod
    def _get_status(self, job_id: str, parameters: CallParameters) -> JobStatus:
        """Fetch the current state of the write-back job and normalize it."""

    @staticmethod
    def select(
        protocol_version: ProtocolVersion,
        arrow_client: AuthenticatedArrowClient,
        query_runner: QueryRunner,
        termination_flag: TerminationFlag,
        progress_bar_options: dict[str, Any] | None = None,
    ) -> "WriteProtocol":
        return {
            ProtocolVersion.V3: RemoteWriteBackV3(arrow_client, query_runner, termination_flag, progress_bar_options),
            ProtocolVersion.V4: RemoteWriteBackV4(arrow_client, query_runner, termination_flag, progress_bar_options),
        }[protocol_version]

    def _build_call_parameters(
        self,
        graph_name: str,
        job_id: str,
        concurrency: int | None,
        property_overwrites: dict[str, str] | None,
        relationship_type_overwrite: str | None,
    ) -> CallParameters:
        configuration: dict[str, Any] = {}
        if concurrency is not None:
            configuration["concurrency"] = concurrency
        if property_overwrites is not None:
            configuration["writeProperties"] = property_overwrites
        if relationship_type_overwrite is not None:
            configuration["writeRelationshipType"] = relationship_type_overwrite

        return CallParameters(
            graphName=graph_name,
            jobId=job_id,
            arrowConfiguration=build_arrow_config(self._arrow_client),
            configuration=configuration,
        )


class RemoteWriteBackV3(WriteProtocol):
    def _start_job(self, parameters: CallParameters) -> None:
        # V3 has no separate start step — each status call runs the write proc itself.
        return None

    def _get_status(self, job_id: str, parameters: CallParameters) -> JobStatus:
        result = self._query_runner.call_procedure(
            ProtocolVersion.V3.versioned_procedure_name("gds.arrow.write"),
            params=parameters,
            retryable=True,
            logging=False,
            mode=QueryMode.WRITE,
            custom_error=False,
        )
        row = result.iloc[0].to_dict()
        raw_status = row["status"]
        # for self-managed dbs the endpoint doesn't return progress yet
        progress = row.get("progress")
        if progress is None:
            progress = 0.0

        return JobStatus(
            done=raw_status == Status.COMPLETED.name,
            raw_status=raw_status,
            progress=progress,
            error=None,
            result_row=row,
        )


class RemoteWriteBackV4(WriteProtocol):
    def _start_job(self, parameters: CallParameters) -> None:
        # host/port are returned but intentionally ignored: writes always go to the leader.
        self._query_runner.call_procedure(
            ProtocolVersion.V4.versioned_procedure_name("gds.arrow.write"),
            params=parameters,
            retryable=False,
            logging=False,
            mode=QueryMode.WRITE,
            custom_error=False,
        )

    def _get_status(self, job_id: str, parameters: CallParameters) -> JobStatus:
        row: Series[Any] = self._query_runner.run_cypher(
            f"CALL gds.arrow.job.status.v4('{job_id}')",
            QueryType.USER_TRANSPILED,
        ).squeeze()

        raw_status: str = row["status"]
        progress_value: Any = row.get("progress")
        progress: float = 0.0 if progress_value is None else float(progress_value)

        done = raw_status == Status.DONE.name
        result_row = row["result"] if done else {}

        return JobStatus(
            done=done,
            raw_status=raw_status,
            progress=progress,
            error=row["error"],
            result_row=result_row,
        )
