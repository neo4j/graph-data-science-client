from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from pandas import Series
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_fixed

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.call_parameters import CallParameters
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType
from graphdatascience.retry_utils.neo4j_retry_helper import is_retryable_neo4j_exception
from graphdatascience.retry_utils.retry_utils import before_log
from graphdatascience.session.dbms.protocol_resolver import ProtocolVersionResolver
from graphdatascience.session.dbms.protocol_version import ProtocolVersion
from graphdatascience.session.remote_ops.arrow_config import build_arrow_config
from graphdatascience.session.remote_ops.status import Status


@dataclass(frozen=True)
class JobStatus:
    """Protocol-agnostic snapshot of a write-back job's state."""

    done: bool
    status: str
    progress: float
    written_node_properties: int
    written_node_labels: int
    written_relationships: int


class WriteProtocol(ABC):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        query_runner: QueryRunner,
    ):
        self._arrow_client = arrow_client
        self._query_runner = query_runner

    @abstractmethod
    def start_job(
        self,
        graph_name: str,
        job_id: str,
        concurrency: int | None = None,
        property_overwrites: dict[str, str] | None = None,
        relationship_type_overwrite: str | None = None,
        log_progress: bool = True,
    ) -> None:
        """Initial call to start the write-back job. No-op for protocols that combine start+poll."""

    @abstractmethod
    def get_status(self, job_id: str) -> JobStatus:
        """Fetch the current state of the write-back job and normalize it."""

    @staticmethod
    def select(
        arrow_client: AuthenticatedArrowClient,
        query_runner: QueryRunner,
    ) -> "WriteProtocol":
        protocol_version = ProtocolVersionResolver(query_runner).resolve()

        return {
            ProtocolVersion.V3: RemoteWriteBackV3(arrow_client, query_runner),
            ProtocolVersion.V4: RemoteWriteBackV4(arrow_client, query_runner),
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
    def __init__(self, arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner):
        super().__init__(arrow_client, query_runner)
        self._parameter_cache: dict[str, CallParameters] = {}
        self._result_cache: dict[str, JobStatus] = {}

    def start_job(
        self,
        graph_name: str,
        job_id: str,
        concurrency: int | None = None,
        property_overwrites: dict[str, str] | None = None,
        relationship_type_overwrite: str | None = None,
        log_progress: bool = True,
    ) -> None:
        self._result_cache.pop(job_id, None)

        parameters = self._build_call_parameters(
            graph_name, job_id, concurrency, property_overwrites, relationship_type_overwrite
        )

        self._parameter_cache[job_id] = parameters

        self.get_status(job_id)

    def get_status(self, job_id: str) -> JobStatus:
        if job_id in self._result_cache:
            return self._result_cache[job_id]

        result = self._query_runner.call_procedure(
            ProtocolVersion.V3.versioned_procedure_name("gds.arrow.write"),
            params=self._parameter_cache[job_id],
            retryable=True,
            logging=False,
            mode=QueryMode.WRITE,
            custom_error=False,
        )
        row = result.iloc[0].to_dict()

        progress = row.get("progress")
        if progress is None:
            progress = 0.0

        status = JobStatus(
            done=row["status"] == Status.COMPLETED.name,
            status=row["status"],  # type: ignore
            progress=progress,  # type: ignore
            written_node_properties=row.get("writtenNodeProperties"),  # type: ignore
            written_node_labels=row.get("writtenNodeLabels"),  # type: ignore
            written_relationships=row.get("writtenRelationships"),  # type: ignore
        )

        if status.done:
            self._result_cache[job_id] = status

        return status


class RemoteWriteBackV4(WriteProtocol):
    def start_job(
        self,
        graph_name: str,
        job_id: str,
        concurrency: int | None = None,
        property_overwrites: dict[str, str] | None = None,
        relationship_type_overwrite: str | None = None,
        log_progress: bool = True,
    ) -> None:
        logger = logging.getLogger(__name__)

        for attempt in Retrying(
            stop=stop_after_attempt(3),
            wait=wait_fixed(2),
            retry=retry_if_exception(is_retryable_neo4j_exception),
            before=before_log(f"start_job (write-back '{job_id}')", logger, logging.DEBUG),
            reraise=True,
        ):
            with attempt:
                try:
                    self._query_runner.call_procedure(
                        ProtocolVersion.V4.versioned_procedure_name("gds.arrow.write"),
                        params=self._build_call_parameters(
                            graph_name, job_id, concurrency, property_overwrites, relationship_type_overwrite
                        ),
                        retryable=False,
                        logging=False,
                        mode=QueryMode.WRITE,
                        custom_error=False,
                    )
                    return
                except Exception as e:
                    try:
                        status = self.get_status(job_id)
                        logger.debug(
                            f"Write-back job '{job_id}' already started (status: {status.status}). No retry needed."
                        )
                        return
                    except Exception:
                        logger.debug(f"Could not confirm state of write-back job '{job_id}', will retry start_job.")
                        raise e

    def get_status(self, job_id: str) -> JobStatus:
        result = self._query_runner.run_retryable_cypher(
            "CALL gds.arrow.job.status.v4($job_id)",
            QueryType.USER_TRANSPILED,
            params={"job_id": job_id},
        )
        if result.empty:
            raise ValueError(f"Write-back job '{job_id}' not found")
        row: Series[Any] = result.iloc[0]

        if row.get("error") is not None:
            raise Exception(row["error"])

        status: str = row["status"]
        done = status == Status.DONE.name
        progress: float = 0.0 if row.get("progress") is None else float(row.get("progress"))  # type: ignore
        result = row["result"] if done else {}

        return JobStatus(
            done=done,
            status=status,
            progress=progress,
            written_node_properties=result.get("writtenNodeProperties"),  # type: ignore
            written_node_labels=result.get("writtenNodeLabels"),  # type: ignore
            written_relationships=result.get("writtenRelationships"),  # type: ignore
        )
