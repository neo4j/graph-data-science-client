from __future__ import annotations

import dataclasses
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, NamedTuple

from pandas import Timedelta

from graphdatascience.session.cloud_location import CloudLocation

from .session_sizes import SessionMemoryValue


@dataclass(repr=True, frozen=True)
class SessionDetails:
    id: str
    name: str
    instance_id: str | None
    memory: SessionMemoryValue
    status: str
    host: str
    created_at: datetime
    expiry_date: datetime | None
    ttl: timedelta | None
    user_id: str
    project_id: str
    cloud_location: CloudLocation | None = None

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> SessionDetails:
        id = data["id"]
        expiry_date = data.get("expiry_date")
        ttl: Any | None = data.get("ttl")
        instance_id = data.get("instance_id")
        cloud_location = CloudLocation(data["cloud_provider"], data["region"]) if data.get("cloud_provider") else None

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
            project_id=data["project_id"],
            user_id=data["user_id"],
            cloud_location=cloud_location,
        )

    def bolt_connection_url(self) -> str:
        return f"neo4j+s://{self.host}"

    def is_ready(self) -> bool:
        return self.status.lower() == "ready"


@dataclass(repr=True, frozen=True)
class SessionDetailsWithErrors(SessionDetails):
    errors: list[SessionErrorData] | None = None

    @classmethod
    def from_json_with_error(cls, data: dict[str, Any], errors: list[dict[str, Any]]) -> SessionDetailsWithErrors:
        session_errors = [SessionErrorData.from_json(error) for error in errors] if errors else None

        id = data["id"]
        expiry_date = data.get("expiry_date")
        ttl: Any | None = data.get("ttl")
        instance_id = data.get("instance_id")
        cloud_location = CloudLocation(data["cloud_provider"], data["region"]) if data.get("cloud_provider") else None

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
            project_id=data["project_id"],
            user_id=data["user_id"],
            cloud_location=cloud_location,
            errors=session_errors,
        )


@dataclass(repr=True, frozen=True)
class SessionErrorData:
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
    def from_json(cls, json: dict[str, Any]) -> SessionErrorData:
        return cls(
            reason=json["reason"],
            message=json["message"],
        )

    def __str__(self) -> str:
        return f"Reason: {self.reason}, Message: {self.message}"


@dataclass(repr=True, frozen=True)
class InstanceDetails:
    id: str
    name: str
    project_id: str
    cloud_provider: str

    @classmethod
    def fromJson(cls, json: dict[str, Any]) -> InstanceDetails:
        return cls(
            id=json["id"],
            name=json["name"],
            project_id=json["tenant_id"],
            cloud_provider=json["cloud_provider"],
        )


@dataclass(repr=True, frozen=True)
class InstanceSpecificDetails(InstanceDetails):
    status: str
    connection_url: str
    memory: SessionMemoryValue
    type: str | None
    region: str

    @classmethod
    def fromJson(cls, json: dict[str, Any]) -> InstanceSpecificDetails:
        return cls(
            id=json["id"],
            name=json["name"],
            project_id=json["tenant_id"],
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
    estimated_memory: str
    recommended_size: str

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> EstimationDetails:
        fields = dataclasses.fields(cls)
        if any(f.name not in json for f in fields):
            raise RuntimeError(f"Missing required field. Expected `{[f.name for f in fields]}` but got `{json}`")

        return cls(**{f.name: json[f.name] for f in fields})

    def exceeds_recommended(self) -> bool:
        return EstimationDetails._memory_in_bytes(self.estimated_memory) > EstimationDetails._memory_in_bytes(
            self.recommended_size
        )

    @staticmethod
    def _memory_in_bytes(size: str) -> float:
        size_str = size.upper().strip()
        # treat GB, Gi and G the same as its only used for comparing it internally
        size_str = size_str.removesuffix("B").removesuffix("I")

        if size_str.endswith("G"):
            return float(size_str[:-1]) * 1024**3  # 1GB = 1024^3 bytes
        elif size_str.endswith("M"):
            return float(size_str[:-1]) * 1024**2  # 1MB = 1024^2 bytes
        elif size_str.endswith("K"):
            return float(size_str[:-1]) * 1024  # 1KB = 1024 bytes
        else:
            return float(size_str)  # assume bytes


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
class ProjectDetails:
    id: str
    cloud_locations: set[CloudLocation]

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> ProjectDetails:
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
