import uuid
from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.write_job_handle import (
    WriteBackResult,
    WriteJobHandle,
)
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType
from graphdatascience.query_runner.termination_flag import TerminationFlagNoop
from graphdatascience.session.remote_ops.write_protocols import (
    RemoteWriteBackV3,
    RemoteWriteBackV4,
    WriteProtocol,
)
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph_from_db


@pytest.fixture
def projected_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[Graph, None, None]:
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
        f"write-job-handle-{uuid.uuid4()}",
        graph_data,
        project_query,
    ) as G:
        yield G


def _start_property_export(arrow_client: AuthenticatedArrowClient, graph_name: str) -> str:
    return JobClient.run_job(
        arrow_client,
        "v2/graph.nodeProperties.stream",
        {"graphName": graph_name, "nodeProperties": ["prop"]},
    )


def _assert_properties_written_to_db(query_runner: QueryRunner) -> None:
    written = query_runner.run_cypher(
        "MATCH (n) RETURN n.prop AS prop",
        query_type=QueryType.USER_ACTION,
    ).squeeze()

    assert set(written.to_list()) == {42, 43, 44}


def _create_write_handle(
    write_protocol: WriteProtocol,
    graph_name: str,
    job_id: str,
) -> WriteJobHandle:
    return WriteJobHandle.create(
        write_protocol,
        graph_name,
        job_id,
        TerminationFlagNoop(),
        log_progress=False,
    )


@pytest.mark.db_integration
def test_v3_write_job_handle_writes_properties(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, projected_graph: Graph
) -> None:
    job_id = _start_property_export(arrow_client, projected_graph.name())
    protocol = RemoteWriteBackV3(arrow_client, query_runner)

    write_handle = _create_write_handle(protocol, projected_graph.name(), job_id)
    result = write_handle.result()

    assert isinstance(result, WriteBackResult)
    assert result.written_node_properties == 3
    assert result.write_millis >= 0
    _assert_properties_written_to_db(query_runner)


@pytest.mark.db_integration
def test_v4_write_job_handle_writes_properties(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, projected_graph: Graph
) -> None:
    job_id = _start_property_export(arrow_client, projected_graph.name())
    protocol = RemoteWriteBackV4(arrow_client, query_runner)

    write_handle = _create_write_handle(protocol, projected_graph.name(), job_id)
    result = write_handle.result()

    assert result.written_node_properties == 3
    assert result.write_millis >= 0
    _assert_properties_written_to_db(query_runner)


@pytest.mark.db_integration
def test_write_job_handle_id_matches_provided_id(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, projected_graph: Graph
) -> None:
    job_id = _start_property_export(arrow_client, projected_graph.name())
    protocol = WriteProtocol.select(arrow_client, query_runner)

    write_handle = _create_write_handle(protocol, projected_graph.name(), job_id)

    assert write_handle.job_id() == job_id

    write_handle.wait()  # drain so cleanup is well-defined


@pytest.mark.db_integration
def test_write_job_handle_done_and_status_after_wait(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, projected_graph: Graph
) -> None:
    job_id = _start_property_export(arrow_client, projected_graph.name())
    protocol = WriteProtocol.select(arrow_client, query_runner)

    write_handle = _create_write_handle(protocol, projected_graph.name(), job_id)
    write_handle.wait()

    assert write_handle.done() is True
    assert write_handle.status().done is True


@pytest.mark.db_integration
def test_write_job_handle_status_is_cached_when_terminal(
    arrow_client: AuthenticatedArrowClient,
    query_runner: QueryRunner,
    projected_graph: Graph,
) -> None:
    job_id = _start_property_export(arrow_client, projected_graph.name())
    protocol = WriteProtocol.select(arrow_client, query_runner)

    write_handle = _create_write_handle(protocol, projected_graph.name(), job_id)
    write_handle.wait()

    # After the first terminal observation, get_status on the protocol should not be called again.
    from unittest.mock import patch

    with patch.object(protocol, "get_status") as get_status_spy:
        first = write_handle.status()
        second = write_handle.status()

        assert first is second
        get_status_spy.assert_not_called()


@pytest.mark.db_integration
def test_select_returns_v3_or_v4_for_running_server(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner
) -> None:
    """Sanity check: the resolver against a real DBMS gives a supported protocol."""
    protocol = WriteProtocol.select(arrow_client, query_runner)
    assert isinstance(protocol, (RemoteWriteBackV3, RemoteWriteBackV4))
