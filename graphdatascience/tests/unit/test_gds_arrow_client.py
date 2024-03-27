import pytest

from graphdatascience.query_runner.gds_arrow_client import AuthMiddleware


def test_auth_middleware() -> None:
    middleware = AuthMiddleware(("user", "password"))

    first_header = middleware.sending_headers()
    assert first_header == {"authorization": "Basic dXNlcjpwYXNzd29yZA=="}

    middleware.received_headers({"authorization": ["Bearer token"]})
    assert middleware._token == "token"

    second_header = middleware.sending_headers()
    assert second_header == {"authorization": "Bearer token"}

    middleware.received_headers({})
    assert middleware._token == "token"

    second_header = middleware.sending_headers()
    assert second_header == {"authorization": "Bearer token"}


def test_auth_middleware_bad_headers() -> None:
    middleware = AuthMiddleware(("user", "password"))

    with pytest.raises(ValueError, match="Incompatible header value received from server: `12342`"):
        middleware.received_headers({"authorization": [12342]})
