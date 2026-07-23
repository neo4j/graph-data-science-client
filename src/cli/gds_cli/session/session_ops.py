"""Create / reconnect to / delete / list the managed GDS session for a job.

``get_or_create`` is idempotent by session name, and the projected graph lives
server-side in the session catalog. That is what lets the per-step CLI commands
(project / algorithms / writeback) each reconnect independently and still see the
same graph by name.
"""

from __future__ import annotations

from datetime import timedelta

import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_fixed

from gds_cli.common.env import aura_api_credentials_from_env, dbms_connection_info_from_env
from gds_cli.session.config import SessionConfig
from graphdatascience.session import AuraGraphDataScience, CloudLocation, DbmsConnectionInfo, GdsSessions, SessionInfo

# A session can be deleted (e.g. TTL expiry) in the gap between us listing it
# and reconnecting to it; the Aura API surfaces that as a RuntimeError whose
# message literally says "please retry". Retry a handful of times, with a
# short pause, before giving up - this also covers plain network hiccups.
_CONNECT_RETRY_ATTEMPTS = 4
_CONNECT_RETRY_WAIT_SECONDS = 2


def build_sessions() -> GdsSessions:
    """Create a GdsSessions handle from Aura API credentials in the environment."""
    return GdsSessions(api_credentials=aura_api_credentials_from_env())


def _is_retryable_connect_error(exc: BaseException) -> bool:
    if isinstance(exc, requests.exceptions.RequestException):
        return True
    if isinstance(exc, RuntimeError):
        message = str(exc).lower()
        return "please retry" in message or "not found" in message
    return False


@retry(
    reraise=True,
    stop=stop_after_attempt(_CONNECT_RETRY_ATTEMPTS),
    wait=wait_fixed(_CONNECT_RETRY_WAIT_SECONDS),
    retry=retry_if_exception(_is_retryable_connect_error),
)
def _get_or_create_with_retry(
    sessions: GdsSessions,
    *,
    session_name: str,
    memory: str,
    db_connection: DbmsConnectionInfo | None,
    cloud_location: CloudLocation | None,
    ttl: timedelta,
    show_progress: bool,
) -> AuraGraphDataScience:
    # Exactly one of db_connection / cloud_location is set: attached vs standalone.
    return sessions.get_or_create(
        session_name=session_name,
        memory=memory,
        db_connection=db_connection,
        cloud_location=cloud_location,
        ttl=ttl,
        show_progress=show_progress,
    )


def find_session(name: str) -> SessionInfo | None:
    """Best-effort lookup of an existing session by name.

    Returns None both when no session with this name exists and when the
    lookup itself fails (network blip, permission issue) - this is purely
    informational, so it must never block `connect` below from creating/
    reconnecting to the session.
    """
    try:
        return next((info for info in build_sessions().list() if info.name == name), None)
    except Exception:
        return None


def connect(cfg: SessionConfig, name: str, show_progress: bool = True) -> AuraGraphDataScience:
    """Create the session if needed, otherwise reconnect to it; returns the gds handle.

    ``show_progress`` controls the returned client's own job-progress bars
    (projection, algorithm execution, writeback, ...) - separate from and not
    controlled by the CLI's own step-by-step reporting.

    Any tqdm bars those operations render use ``leave=False`` so they clear
    themselves once done (matching the database upload command) instead of
    stacking up above the CLI's own step lines.
    """
    from graphdatascience.progress.progress_bar import TqdmProgressBar

    TqdmProgressBar.set_default_options({"leave": False})

    sessions = build_sessions()
    # Standalone (cloud/region in the config) -> no database; attached -> connect to
    # the DB from the env (requires NEO4J_*). Only one of the two is passed.
    if cfg.is_standalone:
        db_connection: DbmsConnectionInfo | None = None
        cloud_location: CloudLocation | None = CloudLocation(cfg.cloud, cfg.region)  # type: ignore[arg-type]
    else:
        db_connection = dbms_connection_info_from_env()
        cloud_location = None
    gds = _get_or_create_with_retry(
        sessions,
        session_name=name,
        memory=cfg.memory,
        db_connection=db_connection,
        cloud_location=cloud_location,
        ttl=cfg.ttl,
        show_progress=show_progress,
    )
    gds.verify_connectivity()
    return gds


def delete(name: str) -> bool:
    """Delete the session by name. Returns True if a session was deleted."""
    return build_sessions().delete(session_name=name)


def list_sessions() -> list[SessionInfo]:
    """List every GDS session visible to the configured Aura API credentials."""
    return build_sessions().list()
