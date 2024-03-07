from __future__ import annotations
from collections import defaultdict
import dataclasses
from typing import Any, NamedTuple, Set
from attr import dataclass


@dataclass(repr=True, frozen=True)
class SessionDetails:
    id: str
    name: str
    memory: str
    created_at: str


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
    memory: str
    type: str
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
            memory=json.get("memory", ""),
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
    def from_json(cls, json: dict[str, Any]) -> InstanceCreateDetails:
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
    ds_type: str
    regions_per_provider: dict[str, Set[str]]

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> TenantDetails:
        regions_per_provider = defaultdict(set)
        instance_types = set()
        ds_type = None

        for configs in json["instance_configurations"]:
            type = configs["type"]
            if type.split("-")[1] == "ds":
                regions_per_provider[configs["cloud_provider"]].add(configs["region"])
                ds_type = type
            instance_types.add(configs["type"])

        id = json["id"]
        if not ds_type:
            raise RuntimeError(
                f"Tenant with id `{id}` cannot create DS instances. Available instances are `{instance_types}`."
            )

        return cls(
            id=id,
            ds_type=ds_type,
            regions_per_provider=regions_per_provider,
        )