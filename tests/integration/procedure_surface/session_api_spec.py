from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

#: The document version this loader understands. The session API is versionless in
#: practice: the published object is overwritten in place, and additions such as the
#: compute mode and the optional result stay within version 1.
SUPPORTED_API_SPEC_VERSION = 1

_REF_PREFIX = "#/$defs/"


class SchemaRef(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    ref: str = Field(alias="$ref")


class ModeEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    modeConfig: SchemaRef | None = None
    result: SchemaRef | None = None


class AlgorithmEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    category: str
    description: str
    endpointName: str
    config: SchemaRef
    modes: dict[str, ModeEntry]


@dataclass(frozen=True)
class SessionEndpointSpec:
    """A single callable endpoint: one algorithm in one execution mode."""

    #: Public endpoint name as published, e.g. `influenceMaximization.celf`.
    endpoint_name: str
    #: Execution mode, or `None` for algorithms without modes (e.g. model training).
    mode: str | None
    config: dict[str, Any]
    mode_config: dict[str, Any] | None
    result: dict[str, Any] | None

    @property
    def name(self) -> str:
        """The public path, e.g. `influenceMaximization.celf.mutate`."""
        return f"{self.endpoint_name}.{self.mode}" if self.mode else self.endpoint_name


class SessionApiSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    apiSpecVersion: int
    algorithms: dict[str, AlgorithmEntry]
    defs: dict[str, dict[str, Any]]

    def resolve(self, ref: SchemaRef) -> dict[str, Any]:
        if not ref.ref.startswith(_REF_PREFIX):
            raise ValueError(f"Unsupported schema reference `{ref.ref}`; expected `{_REF_PREFIX}...`.")

        name = ref.ref.removeprefix(_REF_PREFIX)
        if name not in self.defs:
            raise ValueError(f"Schema reference `{ref.ref}` does not resolve to any definition.")

        return self.defs[name]

    def endpoints(self) -> list[SessionEndpointSpec]:
        """Every endpoint the spec describes, one per algorithm and execution mode."""
        endpoints: list[SessionEndpointSpec] = []

        for entry in self.algorithms.values():
            config = self.resolve(entry.config)
            if not entry.modes:
                endpoints.append(SessionEndpointSpec(entry.endpointName, None, config, None, None))
                continue

            for mode, mode_entry in entry.modes.items():
                mode_config = self.resolve(mode_entry.modeConfig) if mode_entry.modeConfig else None
                result = self.resolve(mode_entry.result) if mode_entry.result else None
                endpoints.append(
                    SessionEndpointSpec(
                        entry.endpointName,
                        mode,
                        config,
                        mode_config,
                        result,
                    )
                )

        return endpoints


def resolve_spec_from_file(file_path: Path) -> SessionApiSpec:
    with open(file_path) as f:
        data = json.load(f)

    try:
        spec = SessionApiSpec(**data)
    except ValidationError as e:
        raise ValueError(f"`{file_path}` is not a valid GDS Sessions API spec:\n{e}") from e

    if spec.apiSpecVersion != SUPPORTED_API_SPEC_VERSION:
        raise ValueError(
            f"Unsupported GDS Sessions API spec version `{spec.apiSpecVersion}` "
            f"(this client understands version `{SUPPORTED_API_SPEC_VERSION}`). "
            "Update the graphdatascience client."
        )

    return spec
