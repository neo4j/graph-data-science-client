from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from graphdatascience.session.aura_api_responses import SessionDetails, SessionDetailsWithErrors, SessionErrorData
from graphdatascience.session.cloud_location import CloudLocation
from graphdatascience.session.session_sizes import SessionMemoryValue


@dataclass(frozen=True)
class SessionInfo:
    """
    Represents information about a session.

    Attributes:
        id (str): The ID of the session.
        name (str): The name of the session.
        memory (str): The size of the session.
        instance_id (str | None): The ID of the AuraDB instance the session is attached to.
        status (str): The status of the session.
        expiry_date (datetime | None): The date the session expires. This is a fixed limit regardless of the user-defined TTL.
        created_at (datetime): The date the session was created.
        user_id (str): The Aura console user-id of the user who created the session.
        cloud_location (CloudLocation | None): The provider and region in which the session is located at.
        ttl (timedelta | None): The time until the session is deleted if unused. The TTL gets renewed on every activity. Rounded down to the nearest minute.
        errors (list[SessionError]): The list of errors related to the session.
    """

    id: str
    name: str
    memory: SessionMemoryValue
    instance_id: str | None
    status: str
    expiry_date: datetime | None
    created_at: datetime
    user_id: str
    cloud_location: CloudLocation | None
    ttl: timedelta | None = None
    errors: list[SessionErrorData] | None = None

    @classmethod
    def from_session_details(cls, details: SessionDetailsWithErrors | SessionDetails) -> SessionInfo:
        errors: list[SessionErrorData] | None = None
        if isinstance(details, SessionDetailsWithErrors):
            errors = details.errors

        return SessionInfo(
            id=details.id,
            name=details.name,
            memory=details.memory,
            instance_id=details.instance_id,
            status=details.status,
            expiry_date=details.expiry_date,
            created_at=details.created_at,
            user_id=details.user_id,
            cloud_location=details.cloud_location,
            ttl=details.ttl,
            errors=errors,
        )
