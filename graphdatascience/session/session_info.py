from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from graphdatascience.session.aura_api_responses import SessionDetails
from graphdatascience.session.cloud_location import CloudLocation
from graphdatascience.session.session_sizes import SessionMemoryValue


@dataclass(frozen=True)
class SessionInfo:
    """
    Represents information about a session.

    Attributes:
        name (str): The name of the session.
        memory (str): The size of the session.
        instance_id (Optional[str]): The ID of the AuraDB instance the session is attached to.
        status (str): The status of the session.
        expiry_date (Optional[datetime]): The date the session expires.
        created_at (datetime): The date the session was created.
        user_id (str): The Aura console user-id of the user who created the session.
        cloud_location (Optional[CloudLocation]): The provider and region in which the session is located at.
        ttl (Optional[timedelta]): The time until the session is deleted if unused. The TTL gets renewed on every activity.
    """

    name: str
    memory: SessionMemoryValue
    instance_id: Optional[str]
    status: str
    expiry_date: Optional[datetime]
    created_at: datetime
    user_id: str
    cloud_location: Optional[CloudLocation]
    ttl: Optional[timedelta] = None

    @classmethod
    def from_session_details(cls, details: SessionDetails) -> SessionInfo:
        return SessionInfo(
            name=details.name,
            memory=details.memory,
            instance_id=details.instance_id,
            status=details.status,
            expiry_date=details.expiry_date,
            created_at=details.created_at,
            user_id=details.user_id,
            cloud_location=details.cloud_location,
            ttl=details.ttl,
        )
