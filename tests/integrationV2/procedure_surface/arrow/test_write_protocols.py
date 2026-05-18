import uuid
from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.query_runner.protocol.write_protocols import (
    RemoteWriteBackV3,
    RemoteWriteBackV4,
    WriteProtocol,
)
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType
from graphdatascience.query_runner.termination_flag import TerminationFlagNoop
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph_from_db


@pytest.fixture
def projected_graph(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner
) -> Generator[GraphV2, None, None]:
    graph_data = """
        CREATE
        (a {prop: 42}),
        (b {prop: 43}),
        (c {prop: 44})
    """
    project_query = """
            MATCH (source)
            WITH gds.graph.project.remote(source, null, {sourceNodeProperties: properties(source), targetNodeProperties: null}) as g
            RETURN g
        """

    with create_graph_from_db(
        arrow_client,
        query_runner,
        f"write-protocol-{uuid.uuid4()}",
        graph_data,
        project_query,
    ) as G:
        yield G


def _start_property_export(arrow_client: AuthenticatedArrowClient, graph_name: str) -> str:
    return JobClient.run_job(
        arrow_client, "v2/graph.nodeProperties.stream", {"graphName": graph_name, "nodeProperties": ["prop"]}
    )


def _assert_properties_written(query_runner: QueryRunner) -> None:
    written = query_runner.run_cypher(
        "MATCH (n) RETURN n.prop AS prop",
        query_type=QueryType.USER_ACTION,
    ).squeeze()

    assert written.tolist() == [42, 43, 44]


@pytest.mark.db_integration
def test_v3_run_write_back(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, projected_graph: GraphV2
) -> None:
    job_id = _start_property_export(arrow_client, projected_graph.name())

    protocol = RemoteWriteBackV3(arrow_client, query_runner, TerminationFlagNoop())

    result = protocol.run_write_back(graph_name=projected_graph.name(), job_id=job_id, log_progress=False)

    assert result.written_node_properties == 3

    _assert_properties_written(query_runner)


@pytest.mark.db_integration
def test_v4_run_write_back(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, projected_graph: GraphV2
) -> None:
    job_id = _start_property_export(arrow_client, projected_graph.name())

    protocol = RemoteWriteBackV4(arrow_client, query_runner, TerminationFlagNoop())

    result = protocol.run_write_back(graph_name=projected_graph.name(), job_id=job_id, log_progress=False)

    assert result.written_node_properties == 3

    _assert_properties_written(query_runner)


@pytest.mark.db_integration
def test_select_returns_v3_or_v4_for_running_server(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner
) -> None:
    """Sanity check: the resolver against a real DBMS should give us a supported protocol."""
    from graphdatascience.session.dbms.protocol_resolver import ProtocolVersionResolver

    protocol_version = ProtocolVersionResolver(query_runner).resolve()
    protocol = WriteProtocol.select(protocol_version, arrow_client, query_runner, TerminationFlagNoop())
    assert isinstance(protocol, (RemoteWriteBackV3, RemoteWriteBackV4))
