from __future__ import annotations

import dataclasses
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, NamedTuple, Optional

from pandas import Timedelta

from graphdatascience.session.cloud_location import CloudLocation

from .session_sizes import SessionMemoryValue


@dataclass(repr=True, frozen=True)
class SessionDetails:
    id: str
    name: str
    instance_id: Optional[str]
    memory: SessionMemoryValue
    status: str
    host: str
    created_at: datetime
    expiry_date: Optional[datetime]
    ttl: Optional[timedelta]
    user_id: str
    tenant_id: str
    cloud_location: Optional[CloudLocation] = None
    errors: Optional[list[SessionError]] = None

    @classmethod
    def from_json(cls, data: dict[str, Any], errors: list[dict[str, Any]]) -> SessionDetails:
        id = data["id"]
        expiry_date = data.get("expiry_date")
        ttl: Any | None = data.get("ttl")
        instance_id = data.get("instance_id")
        cloud_location = CloudLocation(data["cloud_provider"], data["region"]) if data.get("cloud_provider") else None

        session_errors = [SessionError.from_json(error) for error in errors] if errors else None

        return cls(
            id=id,
            name=data["name"],
            instance_id=instance_id if instance_id else None,
            memory=SessionMemoryValue.fromApiResponse(data["memory"]),
            status=data["status"],
            host=data["host"],
            expiry_date=TimeParser.fromisoformat(expiry_date) if expiry_date else None,
            created_at=TimeParser.fromisoformat(data["created_at"]),
            ttl=Timedelta(ttl).to_pytimedelta() if ttl else None,  # datetime has no support for parsing timedelta
            tenant_id=data["tenant_id"],
            user_id=data["user_id"],
            cloud_location=cloud_location,
            errors=session_errors,
        )

    def bolt_connection_url(self) -> str:
        return f"neo4j+s://{self.host}"

    def is_expired(self) -> bool:
        return self.status == "Expired"


@dataclass(repr=True, frozen=True)
class SessionError:
    """
    Represents information about a session errors.
    Indicates that session is in `Failed` state.

    Attributes:
        message (str): Error message communicated by server.
        reason (str): Error reason. Short identifier of encountered error.
    """

    message: str
    reason: str

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> SessionError:
        return cls(
            reason=json["reason"],
            message=json["message"],
        )


@dataclass(repr=True, frozen=True)
class InstanceDetails:
    id: str
    name: str
    tenant_id: str
    cloud_provider: str

    @classmethod
    def fromJson(cls, json: dict[str, Any]) -> InstanceDetails:
        return cls(
            id=json["id"],
            name=json["name"],
            tenant_id=json["tenant_id"],
            cloud_provider=json["cloud_provider"],
        )


@dataclass(repr=True, frozen=True)
class InstanceSpecificDetails(InstanceDetails):
    status: str
    connection_url: str
    memory: SessionMemoryValue
    type: Optional[str]
    region: str

    @classmethod
    def fromJson(cls, json: dict[str, Any]) -> InstanceSpecificDetails:
        return cls(
            id=json["id"],
            name=json["name"],
            tenant_id=json["tenant_id"],
            cloud_provider=json["cloud_provider"],
            status=json["status"],
            connection_url=json.get("connection_url", ""),
            memory=SessionMemoryValue.fromInstanceSize(json.get("memory")),
            type=json.get("type"),  # business-critical instances did not set the type
            region=json["region"],
        )


@dataclass(repr=True, frozen=True)
class InstanceCreateDetails:
    id: str
    username: str
    password: str
    connection_url: str

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> InstanceCreateDetails:
        fields = dataclasses.fields(cls)
        if any(f.name not in json for f in fields):
            raise RuntimeError(f"Missing required field. Expected `{[f.name for f in fields]}` but got `{json}`")

        return cls(**{f.name: json[f.name] for f in fields})


@dataclass(repr=True, frozen=True)
class EstimationDetails:
    min_required_memory: str
    recommended_size: str
    did_exceed_maximum: bool

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> EstimationDetails:
        fields = dataclasses.fields(cls)
        if any(f.name not in json for f in fields):
            raise RuntimeError(f"Missing required field. Expected `{[f.name for f in fields]}` but got `{json}`")

        return cls(**{f.name: json[f.name] for f in fields})


class WaitResult(NamedTuple):
    connection_url: str
    error: str

    @classmethod
    def from_error(cls, error: str) -> WaitResult:
        return cls(connection_url="", error=error)

    @classmethod
    def from_connection_url(cls, connection_url: str) -> WaitResult:
        return cls(connection_url=connection_url, error="")


@dataclass(repr=True, frozen=True)
class TenantDetails:
    id: str
    cloud_locations: set[CloudLocation]

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> TenantDetails:
        cloud_locations: set[CloudLocation] = set()

        for configs in json["instance_configurations"]:
            # assuming Sessions can be spawned wherever instances can be created
            cloud_locations.add(CloudLocation(configs["cloud_provider"], configs["region"]))

        id = json["id"]

        return cls(
            id=id,
            cloud_locations=cloud_locations,
        )


# datetime.fromisoformat only works with Python version > 3.9
class TimeParser:
    @staticmethod
    def fromisoformat(date: str) -> datetime:
        if sys.version_info >= (3, 11):
            return datetime.fromisoformat(date)
        else:
            # Aura API example: 1970-01-01T00:00:00Z
            return datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
