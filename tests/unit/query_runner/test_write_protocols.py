from unittest.mock import MagicMock

import pytest
from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import (
    AuthenticatedArrowClient,
    ConnectionInfo,
)
from graphdatascience.query_runner.protocol.status import Status
from graphdatascience.query_runner.protocol.write_protocols import (
    JobStatus,
    RemoteWriteBackV3,
    RemoteWriteBackV4,
)
from graphdatascience.server_version.server_version import ServerVersion
from tests.unit.conftest import CollectingQueryRunner


@pytest.fixture
def arrow_client() -> MagicMock:
    client = MagicMock(spec=AuthenticatedArrowClient)
    client.advertised_connection_info.return_value = ConnectionInfo(host="arrow.host", port=1234, encrypted=True)
    client.request_token.return_value = "some-token"
    return client


@pytest.fixture
def qr() -> CollectingQueryRunner:
    return CollectingQueryRunner(ServerVersion(0, 0, 0))


def test_v3_start_job_dispatches_expected_query_and_params(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    qr.add__mock_result("gds.arrow.write.v3", DataFrame([{"status": Status.COMPLETED.name, "progress": 1.0}]))

    protocol = RemoteWriteBackV3(arrow_client, qr)

    protocol.start_job(
        graph_name="myGraph",
        job_id="my-job",
        concurrency=4,
        property_overwrites={"foo": "bar"},
        relationship_type_overwrite="REL",
        log_progress=False,
    )

    assert "gds.arrow.write.v3" in qr.queries[0]
    assert qr.params[0] == {
        "graphName": "myGraph",
        "jobId": "my-job",
        "arrowConfiguration": {
            "host": "arrow.host",
            "port": 1234,
            "token": "some-token",
            "encrypted": True,
        },
        "configuration": {
            "concurrency": 4,
            "writeProperties": {"foo": "bar"},
            "writeRelationshipType": "REL",
        },
    }


def test_v3_start_job_omits_optional_configuration(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    qr.add__mock_result("gds.arrow.write.v3", DataFrame([{"status": Status.COMPLETED.name, "progress": 1.0}]))

    protocol = RemoteWriteBackV3(arrow_client, qr)
    protocol.start_job(graph_name="g", job_id="j")

    assert qr.params[0]["configuration"] == {}


def test_v3_get_status_returns_completed_job_status(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    qr.add__mock_result(
        "gds.arrow.write.v3",
        DataFrame(
            [
                {
                    "status": Status.COMPLETED.name,
                    "progress": 1.0,
                    "writtenNodeProperties": 5,
                    "writtenNodeLabels": 1,
                    "writtenRelationships": 2,
                }
            ]
        ),
    )

    protocol = RemoteWriteBackV3(arrow_client, qr)
    protocol.start_job(graph_name="g", job_id="my-job")
    status = protocol.get_status("my-job")

    assert isinstance(status, JobStatus)
    assert status.done is True
    assert status.status == Status.COMPLETED.name
    assert status.progress == 1.0
    assert status.written_node_properties == 5
    assert status.written_node_labels == 1
    assert status.written_relationships == 2


def test_v3_get_status_running_is_not_done(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    qr.add__mock_result(
        "gds.arrow.write.v3",
        DataFrame([{"status": Status.RUNNING.name, "progress": 0.4}]),
    )

    protocol = RemoteWriteBackV3(arrow_client, qr)
    protocol.start_job(graph_name="g", job_id="my-job")
    status = protocol.get_status("my-job")

    assert status.done is False
    assert status.status == Status.RUNNING.name
    assert status.progress == 0.4


def test_v3_get_status_defaults_missing_progress_to_zero(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    qr.add__mock_result(
        "gds.arrow.write.v3",
        DataFrame([{"status": Status.RUNNING.name, "progress": None}]),
    )

    protocol = RemoteWriteBackV3(arrow_client, qr)
    protocol.start_job(graph_name="g", job_id="my-job")
    status = protocol.get_status("my-job")

    assert status.progress == 0.0


def test_v4_start_job_dispatches_expected_query_and_params(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    qr.add__mock_result("gds.arrow.write.v4", DataFrame([{"host": "leader-host", "port": 7777}]))

    protocol = RemoteWriteBackV4(arrow_client, qr)
    protocol.start_job(
        graph_name="myGraph",
        job_id="my-job",
        concurrency=4,
        property_overwrites={"foo": "bar"},
        relationship_type_overwrite="REL",
        log_progress=False,
    )

    assert "gds.arrow.write.v4" in qr.queries[0]
    assert qr.params[0] == {
        "graphName": "myGraph",
        "jobId": "my-job",
        "arrowConfiguration": {
            "host": "arrow.host",
            "port": 1234,
            "token": "some-token",
            "encrypted": True,
        },
        "configuration": {
            "concurrency": 4,
            "writeProperties": {"foo": "bar"},
            "writeRelationshipType": "REL",
        },
    }


def test_v4_get_status_returns_done_job_status(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    qr.add__mock_result(
        "gds.arrow.job.status.v4",
        DataFrame(
            [
                {
                    "status": Status.DONE.name,
                    "error": None,
                    "progress": 1.0,
                    "result": {
                        "writtenNodeProperties": 5,
                        "writtenNodeLabels": 1,
                        "writtenRelationships": 2,
                        "status": Status.DONE.name,
                        "progress": 1.0,
                    },
                }
            ]
        ),
    )

    protocol = RemoteWriteBackV4(arrow_client, qr)
    status = protocol.get_status("my-job")

    assert isinstance(status, JobStatus)
    assert status.done is True
    assert status.status == Status.DONE.name
    assert status.progress == 1.0
    assert status.written_node_properties == 5
    assert status.written_node_labels == 1
    assert status.written_relationships == 2
    assert "gds.arrow.job.status.v4('my-job')" in qr.queries[0]


def test_v4_get_status_running_is_not_done(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    qr.add__mock_result(
        "gds.arrow.job.status.v4",
        DataFrame([{"status": Status.RUNNING.name, "error": None, "progress": 0.3, "result": None}]),
    )

    protocol = RemoteWriteBackV4(arrow_client, qr)
    status = protocol.get_status("my-job")

    assert status.done is False
    assert status.status == Status.RUNNING.name
    assert status.progress == 0.3


def test_v4_get_status_raises_when_error_field_is_set(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    qr.add__mock_result(
        "gds.arrow.job.status.v4",
        DataFrame([{"status": Status.RUNNING.name, "error": "boom", "progress": 0.0, "result": None}]),
    )

    protocol = RemoteWriteBackV4(arrow_client, qr)

    with pytest.raises(Exception, match="boom"):
        protocol.get_status("my-job")
