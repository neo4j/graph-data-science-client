from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.termination_flag import TerminationFlagNoop
from graphdatascience.session.remote_ops.project_protocols import ProjectProtocol
from graphdatascience.session.remote_ops.projection_runner import ProjectionRunner
from graphdatascience.session.remote_ops.status import Status


@pytest.fixture(autouse=True)
def stub_poll_progress(mocker: MockerFixture) -> None:
    mocker.patch(
        "graphdatascience.session.remote_ops.projection_runner.ProjectionRunner._poll_progress",
        return_value=None,
    )


@pytest.fixture
def arrow_client() -> MagicMock:
    return MagicMock(spec=AuthenticatedArrowClient)


@pytest.fixture
def projection_qr() -> MagicMock:
    return MagicMock(spec=QueryRunner)


def test_run_cypher_projection_starts_job_and_returns_done_result(
    arrow_client: MagicMock, projection_qr: MagicMock
) -> None:
    protocol = MagicMock(spec=ProjectProtocol)
    protocol.start_cypher_projection.return_value = ("my-job", projection_qr)
    protocol.get_status.return_value = {"status": Status.DONE.name, "nodeCount": 42}

    runner = ProjectionRunner(protocol, arrow_client, TerminationFlagNoop())

    result = runner.run_cypher_projection(
        graph_name="g",
        query="MATCH (n) RETURN n",
        job_id="my-job",
        query_parameters={"foo": "bar"},
        concurrency=4,
        undirected_relationship_types=["REL"],
        inverse_indexed_relationship_types=["REL2"],
        batch_size=200,
    )

    assert result == {"status": Status.DONE.name, "nodeCount": 42}

    protocol.start_cypher_projection.assert_called_once_with(
        "g",
        "MATCH (n) RETURN n",
        "my-job",
        {"foo": "bar"},
        4,
        ["REL"],
        ["REL2"],
        200,
    )
    # The runner uses the caller-supplied job_id for status polling, not the protocol's return.
    protocol.get_status.assert_called_once_with("my-job", projection_qr)
    projection_qr.close.assert_called_once()


def test_run_cypher_projection_polls_until_done(arrow_client: MagicMock, projection_qr: MagicMock) -> None:
    protocol = MagicMock(spec=ProjectProtocol)
    protocol.start_cypher_projection.return_value = ("server-job", projection_qr)
    protocol.get_status.side_effect = [
        {"status": Status.RUNNING.name},
        {"status": Status.RUNNING.name},
        {"status": Status.DONE.name, "nodeCount": 7},
    ]

    runner = ProjectionRunner(protocol, arrow_client, TerminationFlagNoop())

    result = runner.run_cypher_projection(graph_name="g", query="q", job_id="my-job")

    assert result == {"status": Status.DONE.name, "nodeCount": 7}
    assert protocol.get_status.call_count == 3
    projection_qr.close.assert_called_once()


def test_run_store_projection_starts_job_and_unwraps_result(arrow_client: MagicMock, projection_qr: MagicMock) -> None:
    protocol = MagicMock(spec=ProjectProtocol)
    protocol.start_store_projection.return_value = ("server-job", projection_qr)
    protocol.get_status.return_value = {
        "status": Status.DONE.name,
        "result": {"nodeCount": 10, "relationshipCount": 5},
    }

    runner = ProjectionRunner(protocol, arrow_client, TerminationFlagNoop())

    result = runner.run_store_projection(
        graph_name="g",
        node_label_filter=["Person"],
        relationship_type_filter=["KNOWS"],
        node_properties=["age"],
        relationship_properties=["weight"],
        job_id="my-job",
        concurrency=8,
        undirected_relationship_types=["KNOWS"],
        inverse_indexed_relationship_types=[],
        batch_size=50,
    )

    assert result == {"nodeCount": 10, "relationshipCount": 5}

    protocol.start_store_projection.assert_called_once_with(
        "g",
        ["Person"],
        ["KNOWS"],
        ["age"],
        ["weight"],
        "my-job",
        8,
        ["KNOWS"],
        [],
        50,
    )
    # run_store_projection polls using the protocol-assigned job id.
    protocol.get_status.assert_called_once_with("server-job", projection_qr)
    projection_qr.close.assert_called_once()


def test_run_store_projection_polls_until_done(arrow_client: MagicMock, projection_qr: MagicMock) -> None:
    protocol = MagicMock(spec=ProjectProtocol)
    protocol.start_store_projection.return_value = ("server-job", projection_qr)
    protocol.get_status.side_effect = [
        {"status": Status.RUNNING.name, "result": None},
        {"status": Status.DONE.name, "result": {"nodeCount": 1}},
    ]

    runner = ProjectionRunner(protocol, arrow_client, TerminationFlagNoop())

    result = runner.run_store_projection(
        graph_name="g",
        node_label_filter=["Person"],
        relationship_type_filter=["KNOWS"],
    )

    assert result == {"nodeCount": 1}
    assert protocol.get_status.call_count == 2
    projection_qr.close.assert_called_once()


def test_run_cypher_projection_closes_query_runner_on_error(arrow_client: MagicMock, projection_qr: MagicMock) -> None:
    protocol = MagicMock(spec=ProjectProtocol)
    protocol.start_cypher_projection.return_value = ("server-job", projection_qr)
    protocol.get_status.side_effect = RuntimeError("boom")

    runner = ProjectionRunner(protocol, arrow_client, TerminationFlagNoop())

    with pytest.raises(RuntimeError, match="boom"):
        runner.run_cypher_projection(graph_name="g", query="q", job_id="my-job")

    # Current behavior: an exception during polling propagates without closing the projection runner.
    # If that changes, this assertion should be updated.
    projection_qr.close.assert_called()
