from __future__ import annotations

import time

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.query_runner.protocol.write_protocols import WriteProtocol
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.session.dbms.protocol_resolver import ProtocolVersionResolver


class RemoteWriteBackClient:

    @staticmethod
    def create(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> RemoteWriteBackClient:
        protocol_version = ProtocolVersionResolver(query_runner).resolve()
        write_protocol = WriteProtocol.select(protocol_version, arrow_client, query_runner, TerminationFlag.create())
        return RemoteWriteBackClient(write_protocol)

    def __init__(self, write_protocol: WriteProtocol) -> None:
        self._write_protocol = write_protocol

    def write(
        self,
        graph_name: str,
        job_id: str,
        concurrency: int | None = None,
        property_overwrites: dict[str, str] | None = None,
        relationship_type_overwrite: str | None = None,
        log_progress: bool = True,
    ) -> WriteBackResult:
        start_time = time.time()

        result = self._write_protocol.run_write_back(
            graph_name=graph_name,
            job_id=job_id,
            concurrency=concurrency,
            property_overwrites=property_overwrites,
            relationship_type_overwrite=relationship_type_overwrite,
            log_progress=log_progress,
        )

        write_millis = int((time.time() - start_time) * 1000)

        return WriteBackResult(
            writtenNodeProperties=result.written_node_properties,
            writtenNodeLabels=result.written_node_labels,
            writtenRelationships=result.written_relationships,
            writeMillis=write_millis,
            status=result.status,
            progress=result.progress,
        )


class WriteBackResult(BaseResult):
    written_node_properties: int
    written_node_labels: int
    written_relationships: int
    write_millis: int
    status: str
    progress: float
