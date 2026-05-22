from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from pandas import Series

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.call_parameters import CallParameters
from graphdatascience.query_runner.protocol.arrow_config import build_arrow_config
from graphdatascience.query_runner.protocol.status import Status
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType
from graphdatascience.session.dbms.protocol_version import ProtocolVersion


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
        """Optional initial call to start the write-back job. No-op for protocols that combine start+poll."""

    @abstractmethod
    def get_status(self, job_id: str) -> JobStatus:
        """Fetch the current state of the write-back job and normalize it."""

    @staticmethod
    def select(
        protocol_version: ProtocolVersion,
        arrow_client: AuthenticatedArrowClient,
        query_runner: QueryRunner,
    ) -> "WriteProtocol":
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
    def start_job(
        self,
        graph_name: str,
        job_id: str,
        concurrency: int | None = None,
        property_overwrites: dict[str, str] | None = None,
        relationship_type_overwrite: str | None = None,
        log_progress: bool = True,
    ) -> None:
        self._query_runner.call_procedure(
            ProtocolVersion.V3.versioned_procedure_name("gds.arrow.write"),
            params=self._build_call_parameters(
                graph_name, job_id, concurrency, property_overwrites, relationship_type_overwrite
            ),
            retryable=True,
            logging=False,
            mode=QueryMode.WRITE,
            custom_error=False,
        )

    def get_status(self, job_id: str) -> JobStatus:
        result = self._query_runner.call_procedure(
            ProtocolVersion.V3.versioned_procedure_name("gds.arrow.write"),
            params=CallParameters(),
            retryable=True,
            logging=False,
            mode=QueryMode.WRITE,
            custom_error=False,
        )
        row = result.iloc[0].to_dict()

        progress = row.get("progress")
        if progress is None:
            progress = 0.0

        return JobStatus(
            done=row["status"] == Status.COMPLETED.name,
            status=row["status"],  # type: ignore
            progress=progress,  # type: ignore
            written_node_properties=row.get("writtenNodeProperties"),  # type: ignore
            written_node_labels=row.get("writtenNodeLabels"),  # type: ignore
            written_relationships=row.get("writtenRelationships"),  # type: ignore
        )


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

    def get_status(self, job_id: str) -> JobStatus:
        row: Series[Any] = self._query_runner.run_cypher(
            f"CALL gds.arrow.job.status.v4('{job_id}')",
            QueryType.USER_TRANSPILED,
        ).squeeze()

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
