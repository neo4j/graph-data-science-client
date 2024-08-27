from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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
    """

    name: str
    memory: SessionMemoryValue

    @classmethod
    def from_session_details(cls, details: SessionDetails) -> ExtendedSessionInfo:
        return ExtendedSessionInfo(
            name=details.name,
            memory=details.memory,
            instance_id=details.instance_id,
            status=details.status,
            expiry_date=details.expiry_date,
            created_at=details.created_at,
            user_id=details.user_id,
            cloud_location=details.cloud_location,
        )


@dataclass(frozen=True)
class ExtendedSessionInfo(SessionInfo):
    instance_id: Optional[str]
    status: str
    expiry_date: Optional[datetime]
    created_at: datetime
    user_id: str
    cloud_location: Optional[CloudLocation]
