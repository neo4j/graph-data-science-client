import certifi
import pytest
from pyarrow.flight import FlightUnavailableError
from pytest_mock import MockerFixture

from graphdatascience.arrow_client.arrow_authentication import ArrowAuthentication
from graphdatascience.arrow_client.arrow_info import ArrowInfo
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient, ConnectionInfo
from graphdatascience.arrow_client.server_health_check import ServerHealthCheck
from graphdatascience.retry_utils.retry_config import ExponentialWaitConfig, RetryConfigV2
from graphdatascience.session.aura_api import AuraApi
from graphdatascience.session.session_lifecycle_manager import SessionLifecycleManager


@pytest.fixture
def arrow_info() -> ArrowInfo:
    return ArrowInfo(listenAddress="localhost:8491", enabled=True, running=True, versions=["1.0.0"])


@pytest.fixture
def mock_auth() -> ArrowAuthentication:
    class MockAuthentication(ArrowAuthentication):
        def auth_pair(self) -> tuple[str, str]:
            return ("mock_user", "mock_password")

    return MockAuthentication()


def test_create_authenticated_arrow_client(arrow_info: ArrowInfo, mock_auth: ArrowAuthentication) -> None:
    client = AuthenticatedArrowClient(arrow_info.listenAddress, auth=mock_auth, encrypted=True)

    assert client._retry_config.wait_config == ExponentialWaitConfig(multiplier=1, min=1, max=10)
    assert isinstance(client, AuthenticatedArrowClient)
    assert client.connection_info() == ConnectionInfo("localhost", 8491, encrypted=True)


def test_connection_info(arrow_info: ArrowInfo, retry_config_v2: RetryConfigV2) -> None:
    client = AuthenticatedArrowClient(("localhost", 8491), retry_config=retry_config_v2)
    connection_info = client.connection_info()
    assert connection_info == ConnectionInfo("localhost", 8491, encrypted=False)


def test_pickle_roundtrip(arrow_info: ArrowInfo, retry_config_v2: RetryConfigV2) -> None:
    client = AuthenticatedArrowClient(("localhost", 8491), retry_config=retry_config_v2)
    import pickle

    pickled_client = pickle.dumps(client)
    unpickled_client = pickle.loads(pickled_client)
    assert isinstance(unpickled_client, AuthenticatedArrowClient)
    assert unpickled_client.connection_info() == client.connection_info()
    assert unpickled_client._retry_config == client._retry_config


def test_create_windows(
    arrow_info: ArrowInfo, retry_config_v2: RetryConfigV2, mock_auth: ArrowAuthentication, mocker: MockerFixture
) -> None:
    mocker.patch("platform.system", return_value="Windows")

    spy = mocker.spy(certifi, "contents")

    client = AuthenticatedArrowClient(("localhost", 8491), retry_config=retry_config_v2)

    assert spy.call_count == 1
    assert client.connection_info()


def test_do_action_with_retry_reconnects_on_retryable_error(
    retry_config_v2: RetryConfigV2, mocker: MockerFixture
) -> None:
    first_client = mocker.Mock()
    first_client.do_action.side_effect = FlightUnavailableError("Flight server is unavailable")

    second_client = mocker.Mock()
    expected_result = mocker.Mock()
    second_client.do_action.return_value = iter([expected_result])

    instantiate = mocker.patch.object(
        AuthenticatedArrowClient,
        "_instantiate_flight_client",
        side_effect=[first_client, second_client],
    )

    client = AuthenticatedArrowClient(("localhost", 8491), retry_config=retry_config_v2)

    result = client.do_action_with_retry("v2/test.endpoint", {"foo": "bar"})

    assert result == [expected_result]
    assert instantiate.call_count == 2
    first_client.close.assert_called_once()


def test_do_action_with_retry_reports_unhealthy_server(retry_config_v2: RetryConfigV2, mocker: MockerFixture) -> None:
    flight_client = mocker.Mock()
    flight_client.do_action.side_effect = FlightUnavailableError("Flight server is unavailable")
    mocker.patch.object(AuthenticatedArrowClient, "_instantiate_flight_client", return_value=flight_client)

    class SessionIsDead(Exception):
        pass

    class FailedSessionHealthCheck(ServerHealthCheck):
        def raise_if_unhealthy(self) -> None:
            raise SessionIsDead("The session ran out of memory")

    client = AuthenticatedArrowClient(
        ("localhost", 8491), retry_config=retry_config_v2, health_check=FailedSessionHealthCheck()
    )

    with pytest.raises(SessionIsDead, match="The session ran out of memory") as e:
        client.do_action_with_retry("v2/test.endpoint", {"foo": "bar"})

    # the underlying error is kept as the context of the more descriptive one
    assert isinstance(e.value.__context__, FlightUnavailableError)


def test_do_action_with_retry_keeps_error_if_health_check_explains_nothing(
    retry_config_v2: RetryConfigV2, mocker: MockerFixture
) -> None:
    flight_client = mocker.Mock()
    flight_client.do_action.side_effect = FlightUnavailableError("Flight server is unavailable")
    mocker.patch.object(AuthenticatedArrowClient, "_instantiate_flight_client", return_value=flight_client)

    health_check = mocker.Mock(spec=ServerHealthCheck)

    client = AuthenticatedArrowClient(("localhost", 8491), retry_config=retry_config_v2, health_check=health_check)

    with pytest.raises(FlightUnavailableError, match="Flight server is unavailable"):
        client.do_action_with_retry("v2/test.endpoint", {"foo": "bar"})

    health_check.raise_if_unhealthy.assert_called_once()


def test_pickle_roundtrip_keeps_health_check(retry_config_v2: RetryConfigV2) -> None:
    aura_api = AuraApi(client_id="client_id", client_secret="client_secret", project_id="project_id")
    client = AuthenticatedArrowClient(
        ("localhost", 8491),
        retry_config=retry_config_v2,
        health_check=SessionLifecycleManager("session-0", aura_api),
    )
    import pickle

    unpickled_client = pickle.loads(pickle.dumps(client))

    health_check = unpickled_client._health_check
    assert isinstance(health_check, SessionLifecycleManager)
    assert health_check.session_id == "session-0"
