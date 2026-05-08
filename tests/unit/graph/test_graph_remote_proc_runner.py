from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pandas import Series
from pyarrow import flight
from pytest_mock import MockerFixture

from graphdatascience import GraphCreateResult, ServerVersion
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.gds_arrow_client import GdsArrowClient
from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph.graph_remote_proc_runner import GraphRemoteProcRunner
from graphdatascience.query_runner.protocol.project_protocols import ProjectProtocol
from graphdatascience.session.dbms.protocol_version import ProtocolVersion
from tests.unit.conftest import CollectingQueryRunner

SERVER_VERSION = ServerVersion(2, 10, 0)


def _make_runner(
    query_runner: CollectingQueryRunner,
    arrow_client: GdsArrowClient,
    db_query_runner: CollectingQueryRunner | None = None,
    protocol_version: ProtocolVersion = ProtocolVersion.V4,
) -> GraphRemoteProcRunner:
    runner = GraphRemoteProcRunner(query_runner, arrow_client, "gds.graph", SERVER_VERSION)
    # GraphRemoteProcRunner.project() reads `_db_query_runner` and `_resolved_protocol_version`
    # off `self`; in production these come from the wrapping SessionQueryRunner. The unit tests
    # set them explicitly to isolate the runner from session wiring.
    runner._db_query_runner = db_query_runner if db_query_runner is not None else query_runner  # type: ignore[attr-defined]
    runner._resolved_protocol_version = protocol_version  # type: ignore[attr-defined]
    return runner


@pytest.fixture
def query_runner() -> CollectingQueryRunner:
    return CollectingQueryRunner(SERVER_VERSION)


@pytest.fixture
def flight_client() -> MagicMock:
    return MagicMock(spec=AuthenticatedArrowClient)


@pytest.fixture
def arrow_client(flight_client: MagicMock) -> MagicMock:
    client = MagicMock(spec=GdsArrowClient)
    client.flight_client.return_value = flight_client
    return client


def test_project_invokes_protocol_with_expected_arguments(
    mocker: MockerFixture, query_runner: CollectingQueryRunner, arrow_client: MagicMock, flight_client: MagicMock
) -> None:
    protocol = MagicMock(spec=ProjectProtocol)
    protocol.run_cypher_projection.return_value = {"nodeCount": 7}
    select = mocker.patch.object(ProjectProtocol, "select", return_value=protocol)

    runner = _make_runner(query_runner, arrow_client, protocol_version=ProtocolVersion.V4)

    runner.project(
        graph_name="myGraph",
        query="MATCH (n) RETURN n",
        job_id="my-job",
        concurrency=8,
        undirected_relationship_types=["REL"],
        inverse_indexed_relationship_types=["REL2"],
        batch_size=200,
        logging=False,
    )

    select.assert_called_once()
    selected_args = select.call_args.args
    assert selected_args[0] == ProtocolVersion.V4
    assert selected_args[1] is flight_client
    assert selected_args[2] is query_runner

    protocol.run_cypher_projection.assert_called_once_with(
        "myGraph",
        "MATCH (n) RETURN n",
        "my-job",
        8,
        ["REL"],
        ["REL2"],
        200,
        False,
    )


def test_project_returns_graph_create_result(
    mocker: MockerFixture, query_runner: CollectingQueryRunner, arrow_client: MagicMock
) -> None:
    protocol = MagicMock(spec=ProjectProtocol)
    protocol.run_cypher_projection.return_value = {"nodeCount": 42, "relationshipCount": 5}
    mocker.patch.object(ProjectProtocol, "select", return_value=protocol)

    runner = _make_runner(query_runner, arrow_client)

    result = runner.project(graph_name="myGraph", query="MATCH (n) RETURN n", job_id="job-1")

    assert isinstance(result, GraphCreateResult)
    assert isinstance(result.graph, Graph)
    assert result.graph.name() == "myGraph"
    assert isinstance(result.result, Series)
    assert result.result["nodeCount"] == 42
    assert result.result["relationshipCount"] == 5


def test_project_generates_job_id_when_none_provided(
    mocker: MockerFixture, query_runner: CollectingQueryRunner, arrow_client: MagicMock
) -> None:
    protocol = MagicMock(spec=ProjectProtocol)
    protocol.run_cypher_projection.return_value = {}
    mocker.patch.object(ProjectProtocol, "select", return_value=protocol)

    runner = _make_runner(query_runner, arrow_client)

    runner.project(graph_name="g", query="MATCH (n) RETURN n")

    job_id = protocol.run_cypher_projection.call_args.args[2]
    assert isinstance(job_id, str)
    assert len(job_id) > 0


def test_project_passes_through_provided_job_id(
    mocker: MockerFixture, query_runner: CollectingQueryRunner, arrow_client: MagicMock
) -> None:
    protocol = MagicMock(spec=ProjectProtocol)
    protocol.run_cypher_projection.return_value = {}
    mocker.patch.object(ProjectProtocol, "select", return_value=protocol)

    runner = _make_runner(query_runner, arrow_client)

    runner.project(graph_name="g", query="MATCH (n) RETURN n", job_id="explicit-job")

    assert protocol.run_cypher_projection.call_args.args[2] == "explicit-job"


def test_project_uses_default_argument_values(
    mocker: MockerFixture, query_runner: CollectingQueryRunner, arrow_client: MagicMock
) -> None:
    protocol = MagicMock(spec=ProjectProtocol)
    protocol.run_cypher_projection.return_value = {}
    mocker.patch.object(ProjectProtocol, "select", return_value=protocol)

    runner = _make_runner(query_runner, arrow_client)

    runner.project(graph_name="g", query="MATCH (n) RETURN n", job_id="job-1")

    args = protocol.run_cypher_projection.call_args.args
    # graph_name, query, job_id, concurrency, undirected, inverse_indexed, batch_size, logging
    assert args == ("g", "MATCH (n) RETURN n", "job-1", 4, None, None, None, True)


def test_project_selects_protocol_v3(
    mocker: MockerFixture, query_runner: CollectingQueryRunner, arrow_client: MagicMock
) -> None:
    protocol = MagicMock(spec=ProjectProtocol)
    protocol.run_cypher_projection.return_value = {}
    select = mocker.patch.object(ProjectProtocol, "select", return_value=protocol)

    runner = _make_runner(query_runner, arrow_client, protocol_version=ProtocolVersion.V3)

    runner.project(graph_name="g", query="MATCH (n) RETURN n", job_id="job-1")

    assert select.call_args.args[0] == ProtocolVersion.V3


def test_project_translates_flight_runtime_error(
    mocker: MockerFixture, query_runner: CollectingQueryRunner, arrow_client: MagicMock
) -> None:
    protocol = MagicMock(spec=ProjectProtocol)
    raw_message = "Flight RPC failed with message: org.apache.arrow.flight.FlightRuntimeException: graph already exists"
    protocol.run_cypher_projection.side_effect = flight.FlightServerError(raw_message)
    mocker.patch.object(ProjectProtocol, "select", return_value=protocol)

    runner = _make_runner(query_runner, arrow_client)

    with pytest.raises(flight.FlightServerError) as excinfo:
        runner.project(graph_name="g", query="MATCH (n) RETURN n", job_id="job-1")

    assert "graph already exists" in str(excinfo.value)
    assert "FlightRuntimeException" not in str(excinfo.value)


def test_project_propagates_non_flight_exception(
    mocker: MockerFixture, query_runner: CollectingQueryRunner, arrow_client: MagicMock
) -> None:
    protocol = MagicMock(spec=ProjectProtocol)
    protocol.run_cypher_projection.side_effect = RuntimeError("boom")
    mocker.patch.object(ProjectProtocol, "select", return_value=protocol)

    runner = _make_runner(query_runner, arrow_client)

    with pytest.raises(RuntimeError, match="boom"):
        runner.project(graph_name="g", query="MATCH (n) RETURN n", job_id="job-1")
