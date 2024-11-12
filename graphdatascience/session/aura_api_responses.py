from __future__ import annotations

import dataclasses
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, NamedTuple, Optional, Set

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

    @classmethod
    def from_json(cls, json: Dict[str, Any]) -> SessionDetails:
        expiry_date = json.get("expiry_date")
        ttl: Any | None = json.get("ttl")
        instance_id = json.get("instance_id")
        cloud_location = CloudLocation(json["cloud_provider"], json["region"]) if json.get("cloud_provider") else None

        return cls(
            id=json["id"],
            name=json["name"],
            instance_id=instance_id if instance_id else None,
            memory=SessionMemoryValue.fromApiResponse(json["memory"]),
            status=json["status"],
            host=json["host"],
            expiry_date=TimeParser.fromisoformat(expiry_date) if expiry_date else None,
            created_at=TimeParser.fromisoformat(json["created_at"]),
            ttl=Timedelta(ttl).to_pytimedelta() if ttl else None,  # datetime has no support for parsing timedelta
            tenant_id=json["tenant_id"],
            user_id=json["user_id"],
            cloud_location=cloud_location,
        )

    def bolt_connection_url(self) -> str:
        return f"neo4j+s://{self.host}"

    def is_expired(self) -> bool:
        return self.status == "Expired"


@dataclass(repr=True, frozen=True)
class InstanceDetails:
    id: str
    name: str
    tenant_id: str
    cloud_provider: str

    @classmethod
    def fromJson(cls, json: Dict[str, Any]) -> InstanceDetails:
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
    type: str
    region: str

    @classmethod
    def fromJson(cls, json: Dict[str, Any]) -> InstanceSpecificDetails:
        return cls(
            id=json["id"],
            name=json["name"],
            tenant_id=json["tenant_id"],
            cloud_provider=json["cloud_provider"],
            status=json["status"],
            connection_url=json.get("connection_url", ""),
            memory=SessionMemoryValue.fromInstanceSize(json.get("memory")),
            type=json["type"],
            region=json["region"],
        )


@dataclass(repr=True, frozen=True)
class InstanceCreateDetails:
    id: str
    username: str
    password: str
    connection_url: str

    @classmethod
    def from_json(cls, json: Dict[str, Any]) -> InstanceCreateDetails:
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
    def from_json(cls, json: Dict[str, Any]) -> EstimationDetails:
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
    cloud_locations: Set[CloudLocation]

    @classmethod
    def from_json(cls, json: Dict[str, Any]) -> TenantDetails:
        cloud_locations: Set[CloudLocation] = set()

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
