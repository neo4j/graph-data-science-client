import pytest
from pyarrow._flight import FlightInternalError, FlightTimedOutError, FlightUnavailableError

from graphdatascience.arrow_client.arrow_authentication import ArrowAuthentication
from graphdatascience.arrow_client.arrow_info import ArrowInfo
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient, ConnectionInfo
from graphdatascience.retry_utils.retry_config import ExponentialWaitConfig, RetryConfigV2, StopConfig


@pytest.fixture
def arrow_info() -> ArrowInfo:
    return ArrowInfo(listenAddress="localhost:8491", enabled=True, running=True, versions=["1.0.0"])


@pytest.fixture
def retry_config() -> RetryConfigV2:
    return RetryConfigV2(
        retryable_exceptions=[
            FlightTimedOutError,
            FlightUnavailableError,
            FlightInternalError,
        ],
        stop_config=StopConfig(after_delay=10, after_attempt=5),
        wait_config=ExponentialWaitConfig(multiplier=1, min=1, max=10),
    )


@pytest.fixture
def mock_auth() -> ArrowAuthentication:
    class MockAuthentication(ArrowAuthentication):
        def auth_pair(self) -> tuple[str, str]:
            return ("mock_user", "mock_password")

    return MockAuthentication()


def test_create_authenticated_arrow_client(arrow_info: ArrowInfo, mock_auth: ArrowAuthentication) -> None:
    client = AuthenticatedArrowClient.create(arrow_info=arrow_info, auth=mock_auth, encrypted=True)

    assert client._retry_config.wait_config == ExponentialWaitConfig(multiplier=1, min=1, max=10)
    assert isinstance(client, AuthenticatedArrowClient)
    assert client.connection_info() == ConnectionInfo("localhost", 8491, encrypted=True)


def test_connection_info(arrow_info: ArrowInfo, retry_config: RetryConfigV2) -> None:
    client = AuthenticatedArrowClient(host="localhost", port=8491, retry_config=retry_config)
    connection_info = client.connection_info()
    assert connection_info == ConnectionInfo("localhost", 8491, encrypted=False)


def test_pickle_roundtrip(arrow_info: ArrowInfo, retry_config: RetryConfigV2) -> None:
    client = AuthenticatedArrowClient(host="localhost", port=8491, retry_config=retry_config)
    import pickle

    pickled_client = pickle.dumps(client)
    unpickled_client = pickle.loads(pickled_client)
    assert isinstance(unpickled_client, AuthenticatedArrowClient)
    assert unpickled_client.connection_info() == client.connection_info()
    assert unpickled_client._retry_config == client._retry_config
