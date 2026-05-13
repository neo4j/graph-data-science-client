from io import StringIO
from unittest.mock import MagicMock

import pytest
from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import (
    AuthenticatedArrowClient,
    ConnectionInfo,
)
from graphdatascience.query_runner.protocol.status import Status
from graphdatascience.query_runner.protocol.write_protocols import (
    RemoteWriteBackV3,
    RemoteWriteBackV4,
    WriteProtocol,
)
from graphdatascience.query_runner.termination_flag import TerminationFlagNoop
from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.session.dbms.protocol_version import ProtocolVersion
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


def test_select_returns_v3(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    protocol = WriteProtocol.select(ProtocolVersion.V3, arrow_client, qr, TerminationFlagNoop())
    assert isinstance(protocol, RemoteWriteBackV3)


def test_select_returns_v4(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    protocol = WriteProtocol.select(ProtocolVersion.V4, arrow_client, qr, TerminationFlagNoop())
    assert isinstance(protocol, RemoteWriteBackV4)


def test_select_unsupported_version_raises(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    with pytest.raises(KeyError):
        WriteProtocol.select(ProtocolVersion.V1, arrow_client, qr, TerminationFlagNoop())
    with pytest.raises(KeyError):
        WriteProtocol.select(ProtocolVersion.V2, arrow_client, qr, TerminationFlagNoop())


def test_v3_run_write_back_dispatches_expected_query_and_params(
    arrow_client: MagicMock, qr: CollectingQueryRunner
) -> None:
    qr.add__mock_result(
        "gds.arrow.write.v3",
        DataFrame([{"status": Status.COMPLETED.name, "progress": 1.0, "writtenNodeProperties": 5}]),
    )

    protocol = RemoteWriteBackV3(arrow_client, qr, TerminationFlagNoop())

    result = protocol.run_write_back(
        graph_name="myGraph",
        job_id="my-job",
        concurrency=4,
        property_overwrites={"foo": "bar"},
        relationship_type_overwrite="REL",
        log_progress=False,
    )

    assert result.status == Status.COMPLETED.name

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


def test_v3_run_write_back_polls_until_completed(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    qr.add__mock_result(
        "gds.arrow.write.v3",
        [
            DataFrame([{"status": Status.RUNNING.name, "progress": 0.3}]),
            DataFrame([{"status": Status.RUNNING.name, "progress": 0.6}]),
            DataFrame([{"status": Status.COMPLETED.name, "progress": 1.0}]),
        ],
    )

    protocol = RemoteWriteBackV3(arrow_client, qr, TerminationFlagNoop())

    protocol.run_write_back(graph_name="g", job_id="my-job", log_progress=False)

    assert len(qr.queries) == 3
    assert all("gds.arrow.write.v3" in q for q in qr.queries)


def test_write_back_v3_progress_logging() -> None:
    with StringIO() as pbarOutputStream:
        qr = CollectingQueryRunner(ServerVersion(0, 0, 0))
        qr.add__mock_result("gds.arrow.write.v3", DataFrame([{"status": Status.COMPLETED.name, "progress": 1.0}]))

        client = MagicMock(spec=AuthenticatedArrowClient)
        client.advertised_connection_info.return_value = ConnectionInfo(host="h", port=1, encrypted=False)
        client.request_token.return_value = "t"

        wp = RemoteWriteBackV3(
            client, qr, TerminationFlagNoop(), progress_bar_options={"file": pbarOutputStream, "mininterval": 0}
        )

        wp.run_write_back(graph_name="myGraph", job_id="myJob", log_progress=True)

        bar_output = pbarOutputStream.getvalue().split("\r")

        assert any(
            ["Write-Back (graph: myGraph):   0%|          | 0.0/100 [00:00<?, ?%/s]" in line for line in bar_output]
        ), bar_output
        assert any(["Write-Back (graph: myGraph): 100%|##########| 100.0/100" in line for line in bar_output])


def test_write_back_v3_progress_logging_without_bar() -> None:
    # for self-managed dbs the endpoint doesn't return the progress yet
    with StringIO() as pbarOutputStream:
        qr = CollectingQueryRunner(ServerVersion(0, 0, 0))
        qr.add__mock_result("gds.arrow.write.v3", DataFrame([{"status": Status.COMPLETED.name}]))

        client = MagicMock(spec=AuthenticatedArrowClient)
        client.advertised_connection_info.return_value = ConnectionInfo(host="h", port=1, encrypted=False)
        client.request_token.return_value = "t"

        wp = RemoteWriteBackV3(
            client, qr, TerminationFlagNoop(), progress_bar_options={"file": pbarOutputStream, "mininterval": 0}
        )

        wp.run_write_back(graph_name="myGraph", job_id="myJob", log_progress=True)

        bar_output = pbarOutputStream.getvalue().split("\r")

        assert any(
            ["Write-Back (graph: myGraph):   0%|          | 0.0/100 [00:00<?, ?%/s]" in line for line in bar_output]
        ), bar_output
        assert any(["Write-Back (graph: myGraph): 100%|##########| 100.0/100" in line for line in bar_output])


def test_write_back_v3_progress_logging_aborted() -> None:
    with StringIO() as pbarOutputStream:
        qr = CollectingQueryRunner(ServerVersion(0, 0, 0))
        qr.add__mock_result("gds.arrow.write.v3", ValueError("Job aborted"))

        client = MagicMock(spec=AuthenticatedArrowClient)
        client.advertised_connection_info.return_value = ConnectionInfo(host="h", port=1, encrypted=False)
        client.request_token.return_value = "t"

        wp = RemoteWriteBackV3(
            client, qr, TerminationFlagNoop(), progress_bar_options={"file": pbarOutputStream, "mininterval": 0}
        )

        with pytest.raises(ValueError, match="Job aborted"):
            wp.run_write_back(graph_name="myGraph", job_id="myJob", log_progress=True)

        bar_output = pbarOutputStream.getvalue().split("\r")

        assert any(
            ["Write-Back (graph: myGraph):   0%|          | 0.0/100 [00:00<?, ?%/s]" in line for line in bar_output]
        ), bar_output
        assert any(["status: FAILED" in line for line in bar_output]), bar_output


def test_v4_run_write_back_dispatches_expected_query_and_params(
    arrow_client: MagicMock, qr: CollectingQueryRunner
) -> None:
    qr.add__mock_result(
        "gds.arrow.write.v4",
        DataFrame([{"host": "leader-host", "port": 7777}]),
    )
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
                        "writtenNodeLabels": 0,
                        "writtenRelationships": 0,
                        "status": Status.DONE.name,
                        "progress": 1.0,
                    },
                }
            ]
        ),
    )

    protocol = RemoteWriteBackV4(arrow_client, qr, TerminationFlagNoop())

    result = protocol.run_write_back(
        graph_name="myGraph",
        job_id="my-job",
        concurrency=4,
        property_overwrites={"foo": "bar"},
        relationship_type_overwrite="REL",
        log_progress=False,
    )

    assert result.written_node_properties == 5
    assert result.status == Status.DONE.name

    start_query = qr.queries[0]
    assert "gds.arrow.write.v4" in start_query
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

    status_query = qr.queries[1]
    assert "gds.arrow.job.status.v4('my-job')" in status_query


def test_v4_run_write_back_polls_until_done(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    qr.add__mock_result(
        "gds.arrow.write.v4",
        DataFrame([{"host": "leader-host", "port": 7777}]),
    )
    qr.add__mock_result(
        "gds.arrow.job.status.v4",
        [
            DataFrame(
                [{"status": Status.RUNNING.name, "error": None, "progress": 0.2, "result": None}]
            ),
            DataFrame(
                [{"status": Status.RUNNING.name, "error": None, "progress": 0.6, "result": None}]
            ),
            DataFrame(
                [
                    {
                        "status": Status.DONE.name,
                        "error": None,
                        "progress": 1.0,
                        "result": {
                            "writtenNodeProperties": 1,
                            "writtenNodeLabels": 0,
                            "writtenRelationships": 0,
                            "status": Status.DONE.name,
                            "progress": 1.0,
                        },
                    }
                ]
            ),
        ],
    )

    protocol = RemoteWriteBackV4(arrow_client, qr, TerminationFlagNoop())

    result = protocol.run_write_back(graph_name="g", job_id="my-job", log_progress=False)

    assert result.written_node_properties == 1

    status_queries = [q for q in qr.queries if "gds.arrow.job.status.v4" in q]
    assert len(status_queries) == 3


def test_v4_run_write_back_raises_when_status_has_error(
    arrow_client: MagicMock, qr: CollectingQueryRunner
) -> None:
    qr.add__mock_result(
        "gds.arrow.write.v4",
        DataFrame([{"host": "leader-host", "port": 7777}]),
    )
    qr.add__mock_result(
        "gds.arrow.job.status.v4",
        DataFrame([{"status": Status.RUNNING.name, "error": "boom", "progress": 0.0, "result": None}]),
    )

    protocol = RemoteWriteBackV4(arrow_client, qr, TerminationFlagNoop())

    with pytest.raises(Exception, match="boom"):
        protocol.run_write_back(graph_name="g", job_id="my-job", log_progress=False)


def test_write_back_v4_progress_logging() -> None:
    with StringIO() as pbarOutputStream:
        qr = CollectingQueryRunner(ServerVersion(0, 0, 0))
        qr.add__mock_result("gds.arrow.write.v4", DataFrame([{"host": "leader-host", "port": 7777}]))
        qr.add__mock_result(
            "gds.arrow.job.status.v4",
            DataFrame(
                [
                    {
                        "status": Status.DONE.name,
                        "error": None,
                        "progress": 1.0,
                        "result": {
                            "writtenNodeProperties": 1,
                            "writtenNodeLabels": 0,
                            "writtenRelationships": 0,
                            "status": Status.DONE.name,
                            "progress": 1.0,
                        },
                    }
                ]
            ),
        )

        client = MagicMock(spec=AuthenticatedArrowClient)
        client.advertised_connection_info.return_value = ConnectionInfo(host="h", port=1, encrypted=False)
        client.request_token.return_value = "t"

        wp = RemoteWriteBackV4(
            client, qr, TerminationFlagNoop(), progress_bar_options={"file": pbarOutputStream, "mininterval": 0}
        )

        wp.run_write_back(graph_name="myGraph", job_id="myJob", log_progress=True)

        bar_output = pbarOutputStream.getvalue().split("\r")

        assert any(
            ["Write-Back (graph: myGraph):   0%|          | 0.0/100 [00:00<?, ?%/s]" in line for line in bar_output]
        ), bar_output
        assert any(["Write-Back (graph: myGraph): 100%|##########| 100.0/100" in line for line in bar_output])
