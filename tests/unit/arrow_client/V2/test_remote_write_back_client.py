import pytest
from pandas import DataFrame
from pytest_mock import MockerFixture

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient, WriteBackResult
from graphdatascience.query_runner.protocol.arrow_config import build_arrow_config
from graphdatascience.query_runner.protocol.status import Status
from graphdatascience.query_runner.protocol.write_protocols import RemoteWriteBackV3, RemoteWriteBackV4
from tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner


@pytest.fixture
def mock_arrow_client(mocker: MockerFixture) -> AuthenticatedArrowClient:
    client = mocker.Mock(spec=AuthenticatedArrowClient)
    client.connection_info.return_value = mocker.Mock(host="localhost", port=8080, encrypted=False)
    client.advertised_connection_info.return_value = mocker.Mock(host="remote", port=8080, encrypted=False)
    client.request_token.return_value = "test_token"
    return client  # type: ignore


def _v3_query_runner(write_row: dict[str, object] | None = None) -> CollectingQueryRunner:
    row = write_row or {
        "status": Status.COMPLETED.name,
        "progress": 1.0,
        "writtenNodeProperties": 5,
        "writtenNodeLabels": 0,
        "writtenRelationships": 0,
    }
    qr = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {
            "protocol.version": DataFrame([{"version": "v3"}]),
            "gds.arrow.write.v3": DataFrame([row]),
        },
    )
    return qr


def _v4_query_runner(result_payload: dict[str, object] | None = None) -> CollectingQueryRunner:
    payload = result_payload or {
        "writtenNodeProperties": 5,
        "writtenNodeLabels": 0,
        "writtenRelationships": 0,
        "status": Status.DONE.name,
        "progress": 1.0,
    }
    qr = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {
            "protocol.version": DataFrame([{"version": "v4"}]),
            "gds.arrow.write.v4": DataFrame([{"host": "leader-host", "port": 7777}]),
            "gds.arrow.job.status.v4": DataFrame(
                [{"status": Status.DONE.name, "error": None, "progress": 1.0, "result": payload}]
            ),
        },
    )
    return qr


@pytest.fixture
def write_back_client(mock_arrow_client: AuthenticatedArrowClient) -> RemoteWriteBackClient:
    return RemoteWriteBackClient.create(mock_arrow_client, _v3_query_runner())


def test_write_back_client_initialization(write_back_client: RemoteWriteBackClient) -> None:
    assert isinstance(write_back_client, RemoteWriteBackClient)


def test_arrow_configuration(mock_arrow_client: AuthenticatedArrowClient) -> None:
    expected_config = {
        "host": "remote",
        "port": 8080,
        "token": "test_token",
        "encrypted": False,
    }

    config = build_arrow_config(mock_arrow_client)
    assert config == expected_config


def test_resolves_v3_protocol(mock_arrow_client: AuthenticatedArrowClient) -> None:
    client = RemoteWriteBackClient.create(mock_arrow_client, _v3_query_runner())
    assert isinstance(client._write_protocol, RemoteWriteBackV3)


def test_resolves_v4_protocol(mock_arrow_client: AuthenticatedArrowClient) -> None:
    client = RemoteWriteBackClient.create(mock_arrow_client, _v4_query_runner())
    assert isinstance(client._write_protocol, RemoteWriteBackV4)


def test_resolves_unsupported_protocol_raises(mock_arrow_client: AuthenticatedArrowClient) -> None:
    qr = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {"protocol.version": DataFrame([{"version": "v1"}])},
    )
    with pytest.raises(KeyError):
        RemoteWriteBackClient.create(mock_arrow_client, qr)


def test_write_returns_write_back_result_v3(mock_arrow_client: AuthenticatedArrowClient) -> None:
    qr = _v3_query_runner(
        {
            "status": Status.COMPLETED.name,
            "progress": 1.0,
            "writtenNodeProperties": 7,
            "writtenNodeLabels": 2,
            "writtenRelationships": 3,
        }
    )
    client = RemoteWriteBackClient.create(mock_arrow_client, qr)

    result = client.write(graph_name="g", job_id="j", log_progress=False)

    assert isinstance(result, WriteBackResult)
    assert result.written_node_properties == 7
    assert result.written_node_labels == 2
    assert result.written_relationships == 3
    assert result.status == Status.COMPLETED.name
    assert result.progress == 1.0
    assert result.write_millis >= 0


def test_write_returns_write_back_result_v4(mock_arrow_client: AuthenticatedArrowClient) -> None:
    qr = _v4_query_runner(
        {
            "writtenNodeProperties": 9,
            "writtenNodeLabels": 1,
            "writtenRelationships": 4,
            "status": Status.DONE.name,
            "progress": 1.0,
        }
    )
    client = RemoteWriteBackClient.create(mock_arrow_client, qr)

    result = client.write(graph_name="g", job_id="j", log_progress=False)

    assert isinstance(result, WriteBackResult)
    assert result.written_node_properties == 9
    assert result.written_node_labels == 1
    assert result.written_relationships == 4
    assert result.status == Status.DONE.name
    assert result.progress == 1.0
    assert result.write_millis >= 0


def test_write_forwards_arguments_to_v3_proc(mock_arrow_client: AuthenticatedArrowClient) -> None:
    qr = _v3_query_runner()
    client = RemoteWriteBackClient.create(mock_arrow_client, qr)

    client.write(
        graph_name="myGraph",
        job_id="my-job",
        concurrency=4,
        property_overwrites={"foo": "bar"},
        relationship_type_overwrite="REL",
        log_progress=False,
    )

    write_calls = [(q, p) for q, p in zip(qr.queries, qr.params) if "gds.arrow.write.v3" in q]
    assert len(write_calls) == 1

    _, params = write_calls[0]
    assert params == {
        "graphName": "myGraph",
        "jobId": "my-job",
        "arrowConfiguration": {
            "host": "remote",
            "port": 8080,
            "token": "test_token",
            "encrypted": False,
        },
        "configuration": {
            "concurrency": 4,
            "writeProperties": {"foo": "bar"},
            "writeRelationshipType": "REL",
        },
    }


def test_write_omits_optional_configuration_when_not_provided(
    mock_arrow_client: AuthenticatedArrowClient,
) -> None:
    qr = _v3_query_runner()
    client = RemoteWriteBackClient.create(mock_arrow_client, qr)

    client.write(graph_name="g", job_id="j", log_progress=False)

    write_call = next((q, p) for q, p in zip(qr.queries, qr.params) if "gds.arrow.write.v3" in q)
    assert write_call[1]["configuration"] == {}


def test_write_propagates_protocol_errors(mock_arrow_client: AuthenticatedArrowClient) -> None:
    qr = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {
            "protocol.version": DataFrame([{"version": "v3"}]),
            "gds.arrow.write.v3": ValueError("job blew up"),
        },
    )
    client = RemoteWriteBackClient.create(mock_arrow_client, qr)

    with pytest.raises(ValueError, match="job blew up"):
        client.write(graph_name="g", job_id="j", log_progress=False)
