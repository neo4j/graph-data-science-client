"""Standardized GDS job config (see ``cli/session/schema/job-config.schema.json``).

A job describes graph projections, an ordered list of algorithms (each naming
its graph), and optional writebacks. Projection and drop are implicit: the
algorithms are grouped by graph (by each graph's first appearance in the
list), and every graph is handled as one isolated unit of work - projected,
run, written back (if configured), then dropped - before moving on to the
next graph. The caller never declares project/drop steps by hand. Credentials
are never part of this config.

Every config is validated against ``job-config.schema.json`` (the same document
the ``gds-jobs-api`` Go server validates against) before pydantic parsing, so a
config that satisfies one validates the same way against the other.
"""

from __future__ import annotations

import functools
import json
import os
from importlib import resources
from pathlib import Path
from typing import Any, Literal, Optional

import jsonschema
import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

JOB_CONFIG_ENV_VAR = "GDS_JOB_CONFIG"


@functools.lru_cache(maxsize=1)
def _schema() -> dict[str, Any]:
    schema_text = resources.files("gds_cli.session").joinpath("schema", "job-config.schema.json").read_text()
    schema: dict[str, Any] = json.loads(schema_text)
    return schema


def _validate_against_schema(data: Any) -> None:
    jsonschema.Draft202012Validator(_schema()).validate(data)


class SessionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    memory: str
    ttl_minutes: int = Field(ge=1)


class ProjectionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    graph_name: str
    # A projection is either *remote* (a Cypher `query` returning
    # gds.graph.project.remote(...)) or *native* (selected by `node_labels` +
    # `relationship_types`). Exactly one of the two kinds must be given.
    query: Optional[str] = None
    node_labels: list[str] = Field(default_factory=list)
    relationship_types: list[str] = Field(default_factory=list)
    # Native-projection property lists. Ignored for a remote `query`, which
    # declares the properties it needs inside the Cypher itself.
    node_properties: list[str] = Field(default_factory=list)
    relationship_properties: list[str] = Field(default_factory=list)
    # Relationship types to treat as UNDIRECTED (applies to both kinds). Required
    # by algorithms like triangleCount / localClusteringCoefficient, which reject
    # directed projections. Empty means "project as-is (directed)".
    undirected_relationship_types: list[str] = Field(default_factory=list)

    @property
    def is_native(self) -> bool:
        return self.query is None

    @model_validator(mode="after")
    def _check_projection_kind(self) -> "ProjectionConfig":
        has_query = self.query is not None
        has_native = bool(self.node_labels)
        if has_query and has_native:
            raise ValueError(
                f"projection '{self.graph_name}' sets both a remote `query` and native `node_labels`; pick one."
            )
        if not has_query and not has_native:
            raise ValueError(f"projection '{self.graph_name}' needs either a remote `query` or native `node_labels`.")
        if has_query and (self.node_properties or self.relationship_properties):
            raise ValueError(
                f"projection '{self.graph_name}': `node_properties`/`relationship_properties` apply to native "
                f"projections only; a remote `query` declares its properties inside the Cypher."
            )
        if has_native and not self.relationship_types:
            raise ValueError(f"native projection '{self.graph_name}' needs at least one entry in `relationship_types`.")
        return self


class AlgorithmConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    graph_name: str
    mode: Literal["mutate", "write"]
    mutate_property: Optional[str] = None
    write_property: Optional[str] = None
    parameters: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check_property(self) -> "AlgorithmConfig":
        if self.mode == "mutate" and not self.mutate_property:
            raise ValueError(f"algorithm '{self.name}' has mode=mutate but no mutate_property")
        if self.mode == "write" and not self.write_property:
            raise ValueError(f"algorithm '{self.name}' has mode=write but no write_property")
        return self


class WritebackConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    graph_name: str
    node_properties: list[str] = Field(min_length=1)


class JobConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session: SessionConfig
    projections: list[ProjectionConfig] = Field(min_length=1)
    algorithms: list[AlgorithmConfig] = Field(min_length=1)
    writebacks: list[WritebackConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_graph_references(self) -> "JobConfig":
        projected = {p.graph_name for p in self.projections}
        algorithm_graphs = {a.graph_name for a in self.algorithms}
        for algo in self.algorithms:
            if algo.graph_name not in projected:
                raise ValueError(f"algorithm '{algo.name}' references unknown graph '{algo.graph_name}'")
        for wb in self.writebacks:
            if wb.graph_name not in projected:
                raise ValueError(f"writeback references unknown graph '{wb.graph_name}'")
            if wb.graph_name not in algorithm_graphs:
                raise ValueError(
                    f"writeback references graph '{wb.graph_name}', but no algorithm runs on it - "
                    "writeback timing is derived from an algorithm's last reference to the graph"
                )
        return self

    @classmethod
    def _from_data(cls, data: Any) -> "JobConfig":
        _validate_against_schema(data)
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str | Path) -> "JobConfig":
        data = yaml.safe_load(Path(path).expanduser().read_text())
        return cls._from_data(data)

    @classmethod
    def from_env(cls, var: str = JOB_CONFIG_ENV_VAR) -> "JobConfig":
        """Build a config from a YAML document in an environment variable.

        Lets a single k8s Job resource carry its config inline (as a literal env
        var) instead of needing a paired ConfigMap + volume mount.
        """
        raw = os.environ.get(var)
        if not raw:
            raise RuntimeError(f"No --file given and {var!r} is not set")
        return cls._from_data(yaml.safe_load(raw))
