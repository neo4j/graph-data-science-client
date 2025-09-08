from typing import Optional

import pytest
from pandas import DataFrame
from pytest_mock import MockerFixture

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.write_back_client import WriteBackClient
from graphdatascience.tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner


@pytest.fixture
def mock_arrow_client(mocker: MockerFixture) -> AuthenticatedArrowClient:
    client = mocker.Mock(spec=AuthenticatedArrowClient)
    client.connection_info.return_value = mocker.Mock(host="localhost", port=8080, encrypted=False)
    client.advertised_connection_info.return_value = mocker.Mock(host="remote", port=8080, encrypted=False)
    client.request_token.return_value = "test_token"
    return client  # type: ignore


@pytest.fixture
def write_back_client(mock_arrow_client: AuthenticatedArrowClient) -> WriteBackClient:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION,
        {
            "protocol.version": DataFrame([{"version": "v3"}]),
        },
    )
    return WriteBackClient(mock_arrow_client, query_runner)


def test_write_back_client_initialization(write_back_client: WriteBackClient) -> None:
    assert isinstance(write_back_client, WriteBackClient)


def test_arrow_configuration(write_back_client: WriteBackClient, mock_arrow_client: AuthenticatedArrowClient) -> None:
    expected_config = {
        "host": "remote",
        "port": 8080,
        "token": "test_token",
        "encrypted": False,
    }

    config = write_back_client._arrow_configuration()
    assert config == expected_config


def test_write_calls_run_write_back(write_back_client: WriteBackClient, mocker: MockerFixture) -> None:
    graph_name = "test_graph"
    job_id = "123"
    concurrency: Optional[int] = 4

    write_back_client._write_protocol.run_write_back = mocker.Mock()  # type: ignore

    duration = write_back_client.write(graph_name, job_id, concurrency)

    write_back_client._write_protocol.run_write_back.assert_called_once()  # type: ignore
    assert duration >= 0
