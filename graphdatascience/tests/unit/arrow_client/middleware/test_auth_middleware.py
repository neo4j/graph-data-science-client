import base64
from unittest.mock import Mock, patch

import pytest

from graphdatascience.arrow_client.arrow_authentication import ArrowAuthentication
from graphdatascience.arrow_client.middleware.auth_middleware import AuthMiddleware


@pytest.fixture
def mock_auth() -> ArrowAuthentication:
    class MockAuth(ArrowAuthentication):
        def auth_pair(self) -> tuple[str, str]:
            return "username", "password"

    return MockAuth()


@pytest.fixture
def auth_middleware(mock_auth: Mock) -> AuthMiddleware:
    return AuthMiddleware(mock_auth)


def test_token_initially_none(auth_middleware: AuthMiddleware) -> None:
    assert auth_middleware.token() is None


def test_set_token_updates_token_and_timestamp(auth_middleware: AuthMiddleware) -> None:
    with patch("time.time", return_value=1000000):
        auth_middleware._set_token("test_token")
        assert auth_middleware.token() == "test_token"


def test_token_expires_after_10_minutes(auth_middleware: AuthMiddleware) -> None:
    with patch("time.time", side_effect=[1000000, 1000601]):
        auth_middleware._set_token("test_token")
        assert auth_middleware.token() is None


def test_received_headers_sets_bearer_token(auth_middleware: AuthMiddleware) -> None:
    headers = {"authorization": ["Bearer test_token"]}
    auth_middleware.received_headers(headers)
    assert auth_middleware.token() == "test_token"


def test_received_headers_raises_error_with_invalid_header(auth_middleware: AuthMiddleware) -> None:
    headers = {"authorization": [12345]}  # Invalid header value type
    with pytest.raises(ValueError, match="Incompatible header value received from server: `12345`"):
        auth_middleware.received_headers(headers)


def test_sending_headers_with_existing_token(auth_middleware: AuthMiddleware) -> None:
    auth_middleware._set_token("test_token")
    headers = auth_middleware.sending_headers()
    assert headers == {"authorization": "Bearer test_token"}


def test_sending_headers_with_new_basic_auth(auth_middleware: AuthMiddleware, mock_auth: ArrowAuthentication) -> None:
    headers = auth_middleware.sending_headers()
    expected_auth_token = "Basic " + base64.b64encode(b"username:password").decode("ASCII")
    assert headers == {"authorization": expected_auth_token}
