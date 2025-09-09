from __future__ import annotations

import time
from typing import Any, Optional

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.query_runner.protocol.write_protocols import WriteProtocol
from graphdatascience.query_runner.termination_flag import TerminationFlagNoop
from graphdatascience.session.dbms.protocol_resolver import ProtocolVersionResolver


class WriteBackClient:
    def __init__(self, arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner):
        self._arrow_client = arrow_client
        self._query_runner = query_runner

        protocol_version = ProtocolVersionResolver(query_runner).resolve()
        self._write_protocol = WriteProtocol.select(protocol_version)

    # TODO: Add progress logging
    def write(
        self,
        graph_name: str,
        job_id: str,
        concurrency: Optional[int] = None,
        property_overwrites: Optional[dict[str, str]] = None,
        relationship_type_overwrites: Optional[dict[str, str]] = None,
    ) -> WriteBackResult:
        arrow_config = self._arrow_configuration()

        configuration: dict[str, Any] = {}
        if concurrency is not None:
            configuration["concurrency"] = concurrency
        if property_overwrites is not None:
            configuration["writeProperties"] = property_overwrites
        if relationship_type_overwrites is not None:
            configuration["relationshipTypes"] = relationship_type_overwrites

        write_back_params = CallParameters(
            graphName=graph_name,
            jobId=job_id,
            arrowConfiguration=arrow_config,
            configuration=configuration,
        )

        start_time = time.time()

        result = self._write_protocol.run_write_back(
            self._query_runner, write_back_params, None, TerminationFlagNoop()
        ).squeeze()
        write_millis = int((time.time() - start_time) * 1000)

        return WriteBackResult(writeMillis=write_millis, **result.squeeze())

    def _arrow_configuration(self) -> dict[str, Any]:
        connection_info = self._arrow_client.advertised_connection_info()
        token = self._arrow_client.request_token()
        if token is None:
            token = "IGNORED"
        arrow_config = {
            "host": connection_info.host,
            "port": connection_info.port,
            "token": token,
            "encrypted": connection_info.encrypted,
        }

        return arrow_config


class WriteBackResult(BaseResult):
    written_node_properties: int
    written_node_labels: int
    written_relationships: int
    write_millis: int
    status: str
    progress: float
