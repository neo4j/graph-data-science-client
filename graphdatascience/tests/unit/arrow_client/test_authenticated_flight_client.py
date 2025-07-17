# graphdatascience/tests/test_authenticated_flight_client.py
import pytest
from pyarrow._flight import FlightInternalError, FlightTimedOutError, FlightUnavailableError
from tenacity import retry_any, retry_if_exception_type, stop_after_attempt, stop_after_delay, wait_exponential

from graphdatascience.arrow_client.arrow_authentication import ArrowAuthentication
from graphdatascience.arrow_client.arrow_info import ArrowInfo
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient, ConnectionInfo
from graphdatascience.retry_utils.retry_config import RetryConfig


@pytest.fixture
def arrow_info() -> ArrowInfo:
    return ArrowInfo(listenAddress="localhost:8491", enabled=True, running=True, versions=["1.0.0"])


@pytest.fixture
def retry_config() -> RetryConfig:
    return RetryConfig(
        retry=retry_any(
            retry_if_exception_type(FlightTimedOutError),
            retry_if_exception_type(FlightUnavailableError),
            retry_if_exception_type(FlightInternalError),
        ),
        stop=(stop_after_delay(10) | stop_after_attempt(5)),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )


@pytest.fixture
def mock_auth() -> ArrowAuthentication:
    class MockAuthentication(ArrowAuthentication):
        def auth_pair(self) -> tuple[str, str]:
            return ("mock_user", "mock_password")

    return MockAuthentication()


def test_create_authenticated_arrow_client(
    arrow_info: ArrowInfo, retry_config: RetryConfig, mock_auth: ArrowAuthentication
) -> None:
    client = AuthenticatedArrowClient.create(
        arrow_info=arrow_info, auth=mock_auth, encrypted=True, retry_config=retry_config
    )
    assert isinstance(client, AuthenticatedArrowClient)
    assert client.connection_info() == ConnectionInfo("localhost", 8491, encrypted=True)


def test_connection_info(arrow_info: ArrowInfo, retry_config: RetryConfig) -> None:
    client = AuthenticatedArrowClient(host="localhost", port=8491, retry_config=retry_config)
    connection_info = client.connection_info()
    assert connection_info == ConnectionInfo("localhost", 8491, encrypted=False)
