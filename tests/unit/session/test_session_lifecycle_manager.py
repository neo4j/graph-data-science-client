from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from graphdatascience.session.aura_api import AuraApi, AuraApiError, SessionStatusError
from graphdatascience.session.aura_api_responses import SessionDetailsWithErrors, SessionErrorData
from graphdatascience.session.session_lifecycle_manager import SessionLifecycleManager
from graphdatascience.session.session_sizes import SessionMemory


def session_details(status: str, errors: list[SessionErrorData] | None = None) -> SessionDetailsWithErrors:
    return SessionDetailsWithErrors(
        id="ffff0-ffff1",
        name="my-session",
        instance_id="ffff0",
        database_id=None,
        memory=SessionMemory.m_8GB.value,
        status=status,
        created_at=datetime.now(),
        host="foo.bar",
        expiry_date=None,
        ttl=None,
        project_id="project-1",
        user_id="user-1",
        errors=errors,
    )


def lifecycle_manager(details: SessionDetailsWithErrors | None) -> tuple[SessionLifecycleManager, MagicMock]:
    aura_api = MagicMock(spec=AuraApi)
    aura_api.get_session_with_errors.return_value = details
    return SessionLifecycleManager("ffff0-ffff1", aura_api), aura_api


def test_verify_health_of_ready_session() -> None:
    manager, aura_api = lifecycle_manager(session_details("Ready"))

    manager.verify_health()

    aura_api.get_session_with_errors.assert_called_once_with("ffff0-ffff1")


def test_verify_health_of_oomed_session() -> None:
    reason = "OutOfMemory"
    manager, _ = lifecycle_manager(
        session_details("Failed", [SessionErrorData(message="Session reached its memory limit.", reason=reason)])
    )

    with pytest.raises(SessionStatusError) as e:
        manager.verify_health()

    message = str(e.value)
    assert "Session is in an unhealthy state" in message
    assert f"Reason: {reason}, Message: Session reached its memory limit." in message
    assert "Session `my-session` (id `ffff0-ffff1`) has status `Failed` and memory `8GB`." in message
    assert "cannot recover from running out of memory" in message
    assert "GdsSessions.estimate" in message


def test_verify_health_of_failed_session_without_errors() -> None:
    manager, _ = lifecycle_manager(session_details("Failed"))

    with pytest.raises(SessionStatusError) as e:
        manager.verify_health()

    message = str(e.value)
    assert "Session is in an unhealthy state" in message
    assert "has status `Failed` and memory `8GB`." in message


def test_verify_health_of_expired_session() -> None:
    manager, _ = lifecycle_manager(session_details("Expired"))

    with pytest.raises(SessionStatusError) as e:
        manager.verify_health()

    assert "has status `Expired` and memory `8GB`." in str(e.value)


def test_verify_health_of_deleted_session() -> None:
    manager, _ = lifecycle_manager(session_details("deleted"))

    with pytest.raises(SessionStatusError) as e:
        manager.verify_health()

    assert "has status `deleted` and memory `8GB`." in str(e.value)


def test_verify_health_of_missing_session() -> None:
    manager, _ = lifecycle_manager(None)

    with pytest.raises(SessionStatusError) as e:
        manager.verify_health()

    message = str(e.value)
    assert "Session `ffff0-ffff1` does not exist any more." in message
    assert "It was either deleted or expired. Create a new session to continue." in message


def test_verify_health_of_session_which_is_not_ready_yet() -> None:
    manager, _ = lifecycle_manager(session_details("Creating"))

    with pytest.raises(SessionStatusError) as e:
        manager.verify_health()

    assert "The session is not ready to be used yet (status `Creating`)." in str(e.value)


def test_raise_if_unhealthy_reports_session_errors() -> None:
    manager, _ = lifecycle_manager(
        session_details("Failed", [SessionErrorData(message="Session reached its memory limit.", reason="OutOfMemory")])
    )

    with pytest.raises(SessionStatusError):
        manager.raise_if_unhealthy()


def test_raise_if_unhealthy_ignores_unrelated_failures() -> None:
    manager, aura_api = lifecycle_manager(None)
    aura_api.get_session_with_errors.side_effect = AuraApiError("Aura API is down", status_code=500)

    manager.raise_if_unhealthy()
