"""Standardized GDS job config (see ``cli/session/schema/``).

The CLI input is a ``session`` plus a list of ``jobs``. Each **job** is a
self-contained unit of work with its own single graph projection and is
described by the standalone *job spec* (``job-spec.schema.json``) - the same
document the ``gds-jobs-api`` Go server validates a single job against. A job
bundles:

* ``project`` - one graph projection (``cypher`` or ``native``);
* ``compute`` - an ordered list of algorithms to run, each producing a
  ``resultProperty``;
* ``mutate`` - *optional* override forcing extra produced properties to be
  materialized into the in-session graph. Materialization is normally auto-derived:
  a produced property is mutated when a later ``compute`` names it as an input
  (e.g. FastRP's ``featureProperties``), so `mutate` is only needed to force a
  property the automatic rule would miss;
* ``write`` - which produced properties to persist to the database.

A produced property that is written but not materialized (neither auto-derived nor
in ``mutate``) is written directly from the compute result, skipping the mutate
step to save session memory. Projection and drop are implicit: each job's graph is projected, its
computes run (materializing mutates as it goes), its writes persisted, then the
graph is dropped, before the next job starts. Credentials are never part of this
config.

Algorithm parameters use camelCase in the config (matching the client's Cypher
surface); they are collapsed to snake_case before the client call - see
:mod:`gds_cli.session.algorithms`. The whole document is validated against
``job-config.schema.json`` before pydantic parsing.
"""

from __future__ import annotations

import functools
import json
import os
import re
from datetime import timedelta
from importlib import resources
from pathlib import Path
from typing import Any, Literal, Optional

import jsonschema
import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.alias_generators import to_camel

JOB_CONFIG_ENV_VAR = "GDS_JOB_CONFIG"

# `ttl` accepts a whole number of minutes (`30`) or a unit-suffixed duration
# (`30m` / `2h` / `1d`); an unsuffixed string is minutes.
_TTL_PATTERN = re.compile(r"^\s*(\d+)\s*([mhd])?\s*$", re.IGNORECASE)
_TTL_UNIT = {"m": "minutes", "h": "hours", "d": "days"}


def parse_ttl(value: "str | int | timedelta") -> timedelta:
    """Parse a session TTL into a ``timedelta``.

    Accepts an int (minutes), a unit-suffixed string (``30m`` / ``2h`` / ``1d``), a
    bare numeric string (minutes), or a ``timedelta`` (passed through). Must be at
    least one minute.
    """
    if isinstance(value, timedelta):
        delta = value
    elif isinstance(value, bool):  # bool is an int subclass - reject explicitly
        raise ValueError(f"invalid ttl {value!r}: use e.g. '30m', '2h', '1d', or an integer number of minutes.")
    elif isinstance(value, int):
        delta = timedelta(minutes=value)
    else:
        match = _TTL_PATTERN.match(str(value))
        if not match:
            raise ValueError(f"invalid ttl {value!r}: use e.g. '30m', '2h', '1d', or an integer number of minutes.")
        amount = int(match.group(1))
        unit = (match.group(2) or "m").lower()
        delta = timedelta(**{_TTL_UNIT[unit]: amount})
    if delta < timedelta(minutes=1):
        raise ValueError(f"ttl must be at least 1 minute (got {value!r}).")
    return delta


# Keys from the retired flat schema; used to give a clear error if an old config is fed in.
_LEGACY_TOP_LEVEL_KEYS = {"projections", "algorithms", "writebacks"}


@functools.lru_cache(maxsize=1)
def _schema() -> dict[str, Any]:
    schema_text = resources.files("gds_cli.session").joinpath("schema", "job-config.schema.json").read_text()
    schema: dict[str, Any] = json.loads(schema_text)
    return schema


def _validate_against_schema(data: Any) -> None:
    if isinstance(data, dict):
        legacy = _LEGACY_TOP_LEVEL_KEYS.intersection(data)
        if legacy:
            raise ValueError(
                f"This config uses the retired flat schema ({', '.join(sorted(legacy))}). "
                "The job config is now `session` + a list of `jobs` (project/compute/mutate/write) - "
                "see examples/cli/jobs/."
            )
    jsonschema.Draft202012Validator(_schema()).validate(data)


class _CamelModel(BaseModel):
    """Base for job-spec models: accept camelCase (as in YAML) and snake_case."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, extra="forbid")


class SessionConfig(BaseModel):
    # The session block keeps snake_case, so no alias generator here.
    model_config = ConfigDict(extra="forbid")

    # The session *name* is optional. `gds run` uses `--session-name` if given, else
    # this `name`, else a generated throwaway name (deleted after the run); a named
    # session is kept. `gds sessions create/delete` also read/override it.
    name: Optional[str] = None
    memory: str
    # Time-to-live: minutes as an integer (`30`) or a unit-suffixed duration
    # (`30m` / `2h` / `1d`). Stored as a timedelta (see `parse_ttl`).
    ttl: timedelta
    # A *standalone* session (not attached to any database) is created against a
    # cloud location instead of a DB. Set both `cloud` (provider: gcp/aws/azure) and
    # `region`, or neither (attached to the DB from the env). See `is_standalone`.
    cloud: Optional[str] = None
    region: Optional[str] = None

    @field_validator("ttl", mode="before")
    @classmethod
    def _parse_ttl(cls, value: Any) -> timedelta:
        return parse_ttl(value)

    @property
    def is_standalone(self) -> bool:
        return self.cloud is not None

    @model_validator(mode="after")
    def _check_cloud_location(self) -> "SessionConfig":
        if (self.cloud is None) != (self.region is None):
            raise ValueError("session needs both `cloud` and `region` (for a standalone session), or neither.")
        return self


class ProjectSpec(_CamelModel):
    # A projection is `cypher` (a Cypher `query`) or `native` (selected by
    # `node_labels` + `relationship_types`) - both read from the connected DB - or
    # `construct` (built from a graph `file` via gds.graph.construct, no DB needed).
    type: Literal["cypher", "native", "construct"]
    query: Optional[str] = None
    file: Optional[str] = None
    node_labels: list[str] = Field(default_factory=list)
    relationship_types: list[str] = Field(default_factory=list)
    node_properties: list[str] = Field(default_factory=list)
    relationship_properties: list[str] = Field(default_factory=list)
    # Relationship types to treat as UNDIRECTED (applies to all kinds). Required by
    # algorithms like triangleCount / localClusteringCoefficient. Empty => directed.
    undirected_relationship_types: list[str] = Field(default_factory=list)

    @property
    def is_native(self) -> bool:
        return self.type == "native"

    @property
    def is_construct(self) -> bool:
        return self.type == "construct"

    @model_validator(mode="after")
    def _check_projection_kind(self) -> "ProjectSpec":
        native_fields = (
            self.node_labels or self.relationship_types or self.node_properties or self.relationship_properties
        )
        if self.type == "cypher":
            if not self.query:
                raise ValueError("cypher projection needs a `query`.")
            if self.file or native_fields:
                raise ValueError("cypher projection must set only `query` (no `file` or native fields).")
        elif self.type == "construct":
            if not self.file:
                raise ValueError("construct projection needs a graph `file`.")
            if self.query or native_fields:
                raise ValueError("construct projection must set only `file` (no `query` or native fields).")
        else:  # native
            if self.query or self.file:
                raise ValueError("native projection must not set a `query` or `file`.")
            if not self.node_labels:
                raise ValueError("native projection needs at least one entry in `nodeLabels`.")
            if not self.relationship_types:
                raise ValueError("native projection needs at least one entry in `relationshipTypes`.")
        return self


class ComputeSpec(_CamelModel):
    compute: str
    config: dict[str, Any] = Field(default_factory=dict)

    @property
    def algorithm(self) -> str:
        return self.compute

    @property
    def result_property(self) -> str:
        return str(self.config["resultProperty"])

    @property
    def parameters(self) -> dict[str, Any]:
        """Algorithm parameters (config minus the resultProperty selector), camelCase as given."""
        return {k: v for k, v in self.config.items() if k != "resultProperty"}

    @model_validator(mode="after")
    def _check_result_property(self) -> "ComputeSpec":
        rp = self.config.get("resultProperty")
        if not rp or not isinstance(rp, str):
            raise ValueError(f"compute '{self.compute}' needs a string `resultProperty` in its config.")
        return self


class MutateSpec(_CamelModel):
    node_property: str


class WriteSpec(_CamelModel):
    node_property: str
    write_property: Optional[str] = None
    # Standalone sessions only: file (relative to the job config) this property is
    # streamed to - there is no database to write back to. Extension picks the
    # format (.csv or .json); properties sharing a file are written together.
    output_file: Optional[str] = None

    @property
    def target(self) -> str:
        """DB property / output-column name (defaults to the in-graph node property name)."""
        return self.write_property or self.node_property


class JobSpec(_CamelModel):
    project: ProjectSpec
    compute: list[ComputeSpec] = Field(min_length=1)
    mutate: list[MutateSpec] = Field(default_factory=list)
    write: list[WriteSpec] = Field(default_factory=list)

    @property
    def produced_properties(self) -> set[str]:
        return {c.result_property for c in self.compute}

    @property
    def mutated_properties(self) -> set[str]:
        """Produced properties to materialize into the in-session graph.

        The union of the explicit ``mutate`` overrides and the properties
        auto-derived from downstream consumption: walking the computes in order, a
        produced ``resultProperty`` is mutated if a *later* compute names it as an
        input property (e.g. FastRP's ``featureProperties``). See
        :func:`gds_cli.session.algorithms.input_property_references`.
        """
        from gds_cli.session.algorithms import input_property_references

        mutated = {m.node_property for m in self.mutate}
        produced: set[str] = set()
        for spec in self.compute:
            for ref in input_property_references(spec.algorithm, spec.config):
                if ref in produced:
                    mutated.add(ref)
            produced.add(spec.result_property)
        return mutated

    @model_validator(mode="after")
    def _check_property_references(self) -> "JobSpec":
        produced = self.produced_properties
        for m in self.mutate:
            if m.node_property not in produced:
                raise ValueError(
                    f"mutate references property '{m.node_property}', "
                    "which no compute in this job produces (as a resultProperty)."
                )
        for w in self.write:
            if w.node_property not in produced:
                raise ValueError(
                    f"write references property '{w.node_property}', "
                    "which no compute in this job produces (as a resultProperty)."
                )
        return self


class JobsConfig(_CamelModel):
    session: SessionConfig
    jobs: list[JobSpec] = Field(min_length=1)

    @model_validator(mode="after")
    def _check_standalone_projections(self) -> "JobsConfig":
        # Each standalone job overwrites its output files when it streams, so two
        # jobs sharing one would clobber each other - reject that up front. Compared
        # by normalized path so `result.json` and `./result.json` collide too.
        output_file_owner: dict[str, int] = {}
        for i, job in enumerate(self.jobs):
            if self.session.is_standalone:
                # No database: build the graph from a file, and stream each written
                # property to its own output file (there's nothing to write back to).
                if not job.project.is_construct:
                    raise ValueError(
                        f"job {i} uses a '{job.project.type}' projection, but a standalone session "
                        "(with cloud/region) has no database - use a `construct` projection (from a file)."
                    )
                for w in job.write:
                    if not w.output_file:
                        raise ValueError(
                            f"job {i} writes '{w.node_property}' in a standalone session, so that write "
                            "needs an `outputFile` to stream it to."
                        )
                # Register this job's output files (deduped, since one job may write
                # several properties to the same file - that grouping is allowed).
                for output_file in {w.output_file for w in job.write if w.output_file}:
                    normalized = os.path.normpath(output_file)
                    if normalized in output_file_owner:
                        raise ValueError(
                            f"job {i} writes to outputFile '{output_file}', already used by job "
                            f"{output_file_owner[normalized]}; each standalone job overwrites its output "
                            "files, so two jobs cannot share one - give them distinct outputFile paths."
                        )
                    output_file_owner[normalized] = i
            else:
                for w in job.write:
                    if w.output_file is not None:
                        raise ValueError(
                            f"job {i} write '{w.node_property}' sets `outputFile`, which only applies to standalone "
                            "sessions (attached sessions write results back to the database)."
                        )
        return self

    @classmethod
    def _from_data(cls, data: Any) -> "JobsConfig":
        _validate_against_schema(data)
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: str | Path) -> "JobsConfig":
        data = yaml.safe_load(Path(path).expanduser().read_text())
        return cls._from_data(data)

    @classmethod
    def from_env(cls, var: str = JOB_CONFIG_ENV_VAR) -> "JobsConfig":
        """Build a config from a YAML document in an environment variable.

        Lets a single k8s Job resource carry its config inline (as a literal env
        var) instead of needing a paired ConfigMap + volume mount.
        """
        raw = os.environ.get(var)
        if not raw:
            raise RuntimeError(f"No --file given and {var!r} is not set")
        return cls._from_data(yaml.safe_load(raw))
