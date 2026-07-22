from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from gds_cli.session.config import SessionConfig
from gds_cli.session.session_ops import (
    _CONNECT_RETRY_ATTEMPTS,
    build_sessions,
    connect,
    delete,
    find_session,
    list_sessions,
)


@pytest.fixture(autouse=True)
def _aura_credentials_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLIENT_ID", "client-id")
    monkeypatch.setenv("CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("NEO4J_USERNAME", "neo4j")
    monkeypatch.setenv("NEO4J_PASSWORD", "password")
    monkeypatch.setenv("AURA_INSTANCEID", "instance-id")


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Retry tests exercise the real tenacity wait; skip the actual delay.

    tenacity binds its default `sleep` function as a constructor default arg at
    decoration time, so patching the `tenacity.nap.sleep` module attribute after
    the fact has no effect - the already-built `Retrying` instance (exposed as
    `_get_or_create_with_retry.retry`) must be patched directly.
    """
    from gds_cli.session.session_ops import _get_or_create_with_retry

    # tenacity's @retry adds `.retry` (the Retrying instance) at runtime; mypy can't see it.
    monkeypatch.setattr(_get_or_create_with_retry.retry, "sleep", lambda seconds: None)  # type: ignore[attr-defined]


def test_build_sessions_uses_env_credentials() -> None:
    with patch("gds_cli.session.session_ops.GdsSessions") as gds_sessions_cls:
        build_sessions()

    _, kwargs = gds_sessions_cls.call_args
    assert kwargs["api_credentials"].client_id == "client-id"
    assert kwargs["api_credentials"].client_secret == "client-secret"


def test_connect_gets_or_creates_and_verifies() -> None:
    cfg = SessionConfig(name="my-session", memory="2GB", ttl_minutes=30)
    fake_gds = MagicMock()
    fake_sessions = MagicMock()
    fake_sessions.get_or_create.return_value = fake_gds

    with patch("gds_cli.session.session_ops.GdsSessions", return_value=fake_sessions):
        gds = connect(cfg)

    fake_sessions.get_or_create.assert_called_once()
    _, kwargs = fake_sessions.get_or_create.call_args
    assert kwargs["session_name"] == "my-session"
    assert kwargs["memory"] == "2GB"
    assert kwargs["ttl"] == timedelta(minutes=30)
    assert kwargs["show_progress"] is True
    fake_gds.verify_connectivity.assert_called_once()
    assert gds is fake_gds


def test_connect_forwards_show_progress_false() -> None:
    cfg = SessionConfig(name="my-session", memory="2GB", ttl_minutes=30)
    fake_sessions = MagicMock()
    fake_sessions.get_or_create.return_value = MagicMock()

    with patch("gds_cli.session.session_ops.GdsSessions", return_value=fake_sessions):
        connect(cfg, show_progress=False)

    _, kwargs = fake_sessions.get_or_create.call_args
    assert kwargs["show_progress"] is False


def test_connect_retries_on_session_not_found_error() -> None:
    cfg = SessionConfig(name="my-session", memory="2GB", ttl_minutes=30)
    fake_gds = MagicMock()
    fake_sessions = MagicMock()
    transient_error = RuntimeError(
        "Failed to get or create session `my-session`: Session `s-abc` not found -- please retry"
    )
    fake_sessions.get_or_create.side_effect = [transient_error, transient_error, fake_gds]

    with patch("gds_cli.session.session_ops.GdsSessions", return_value=fake_sessions):
        gds = connect(cfg)

    assert fake_sessions.get_or_create.call_count == 3
    assert gds is fake_gds


def test_connect_gives_up_after_max_retries() -> None:
    cfg = SessionConfig(name="my-session", memory="2GB", ttl_minutes=30)
    fake_sessions = MagicMock()
    fake_sessions.get_or_create.side_effect = RuntimeError("Session `s-abc` not found -- please retry")

    with patch("gds_cli.session.session_ops.GdsSessions", return_value=fake_sessions):
        with pytest.raises(RuntimeError, match="please retry"):
            connect(cfg)

    assert fake_sessions.get_or_create.call_count == _CONNECT_RETRY_ATTEMPTS


def test_connect_does_not_retry_non_transient_errors() -> None:
    cfg = SessionConfig(name="my-session", memory="2GB", ttl_minutes=30)
    fake_sessions = MagicMock()
    fake_sessions.get_or_create.side_effect = ValueError("cloud_location must be provided for sessions not attached")

    with patch("gds_cli.session.session_ops.GdsSessions", return_value=fake_sessions):
        with pytest.raises(ValueError):
            connect(cfg)

    fake_sessions.get_or_create.assert_called_once()


def test_find_session_none_when_no_match() -> None:
    cfg = SessionConfig(name="my-session", memory="2GB", ttl_minutes=30)
    fake_sessions = MagicMock()
    fake_sessions.list.return_value = []

    with patch("gds_cli.session.session_ops.GdsSessions", return_value=fake_sessions):
        assert find_session(cfg) is None


def test_find_session_returns_matching_session() -> None:
    cfg = SessionConfig(name="my-session", memory="2GB", ttl_minutes=30)
    fake_sessions = MagicMock()
    matching_session = MagicMock()
    matching_session.name = "my-session"
    matching_session.id = "session-123"
    fake_sessions.list.return_value = [matching_session]

    with patch("gds_cli.session.session_ops.GdsSessions", return_value=fake_sessions):
        found = find_session(cfg)

    assert found is matching_session
    assert found.id == "session-123"


def test_find_session_none_when_check_fails() -> None:
    cfg = SessionConfig(name="my-session", memory="2GB", ttl_minutes=30)
    fake_sessions = MagicMock()
    fake_sessions.list.side_effect = RuntimeError("API unavailable")

    with patch("gds_cli.session.session_ops.GdsSessions", return_value=fake_sessions):
        assert find_session(cfg) is None


def test_delete_calls_sessions_delete() -> None:
    cfg = SessionConfig(name="my-session", memory="2GB", ttl_minutes=30)
    fake_sessions = MagicMock()
    fake_sessions.delete.return_value = True

    with patch("gds_cli.session.session_ops.GdsSessions", return_value=fake_sessions):
        result = delete(cfg)

    fake_sessions.delete.assert_called_once_with(session_name="my-session")
    assert result is True


def test_list_sessions_calls_sessions_list() -> None:
    fake_sessions = MagicMock()
    fake_sessions.list.return_value = []

    with patch("gds_cli.session.session_ops.GdsSessions", return_value=fake_sessions):
        result = list_sessions()

    fake_sessions.list.assert_called_once()
    assert result == []
