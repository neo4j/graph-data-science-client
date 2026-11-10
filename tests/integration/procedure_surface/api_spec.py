from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, ValidationError

#: The document version this loader understands. Both published specs — the GDS
#: Sessions API spec and the GDS plugin API spec — use this envelope: additions such as
#: the compute mode and the optional result stay within version 1. Unknown keys are
#: ignored for the same reason: additive changes within a version must not break loading.
SUPPORTED_API_SPEC_VERSION = 1

_REF_PREFIX = "#/$defs/"


class SchemaRef(BaseModel, extra="ignore", populate_by_name=True):
    ref: str = Field(alias="$ref")


class ModeEntry(BaseModel, extra="ignore"):
    modeConfig: SchemaRef | None = None
    result: SchemaRef | None = None


class AlgorithmEntry(BaseModel, extra="ignore"):
    category: str
    description: str
    endpointName: str
    config: SchemaRef
    modes: dict[str, ModeEntry]


class GraphCatalogEndpointEntry(BaseModel, extra="ignore"):
    """A `gds.graph.*` endpoint.

    The plugin declares some procedures with several modes and the session surface
    publishes one endpoint per operation, so the mode is part of the callable path
    (`gds.graph.nodeLabel.mutate`) rather than a mode of the entry.
    """

    description: str
    endpointName: str
    mode: str | None = None
    config: SchemaRef | None = None
    result: SchemaRef | None = None


@dataclass(frozen=True)
class EndpointSpec:
    """A single callable endpoint: one algorithm in one execution mode."""

    #: Public endpoint name as published, e.g. `influenceMaximization.celf` or `gds.articleRank`.
    endpoint_name: str
    #: Execution mode, or `None` for algorithms without modes (e.g. model training).
    mode: str | None
    config: dict[str, Any]
    mode_config: dict[str, Any] | None
    result: dict[str, Any] | None

    @property
    def name(self) -> str:
        """The public path, e.g. `influenceMaximization.celf.mutate` or `gds.articleRank.mutate`."""
        return f"{self.endpoint_name}.{self.mode}" if self.mode else self.endpoint_name


class ApiSpec(BaseModel, extra="ignore"):
    apiSpecVersion: int
    algorithms: dict[str, AlgorithmEntry]
    graphCatalogEndpoints: dict[str, GraphCatalogEndpointEntry] = {}
    defs: dict[str, dict[str, Any]]

    def resolve(self, ref: SchemaRef) -> dict[str, Any]:
        if not ref.ref.startswith(_REF_PREFIX):
            raise ValueError(f"Unsupported schema reference `{ref.ref}`; expected `{_REF_PREFIX}...`.")

        name = ref.ref.removeprefix(_REF_PREFIX)
        if name not in self.defs:
            raise ValueError(f"Schema reference `{ref.ref}` does not resolve to any definition.")

        return self.defs[name]

    def endpoints(self) -> list[EndpointSpec]:
        """Every endpoint the spec describes, one per algorithm and execution mode."""
        endpoints: list[EndpointSpec] = []

        for entry in self.algorithms.values():
            config = self.resolve(entry.config)
            if not entry.modes:
                endpoints.append(EndpointSpec(entry.endpointName, None, config, None, None))
                continue

            for mode, mode_entry in entry.modes.items():
                mode_config = self.resolve(mode_entry.modeConfig) if mode_entry.modeConfig else None
                result = self.resolve(mode_entry.result) if mode_entry.result else None
                endpoints.append(
                    EndpointSpec(
                        entry.endpointName,
                        mode,
                        config,
                        mode_config,
                        result,
                    )
                )

        for catalog_entry in self.graphCatalogEndpoints.values():
            # The mode is part of the callable path; the session surface already spells
            # it out in the endpoint name, the plugin publishes the procedure name.
            catalog_mode = catalog_entry.mode
            endpoint_name = (
                f"{catalog_entry.endpointName}.{catalog_mode}"
                if catalog_mode and not catalog_entry.endpointName.endswith(f".{catalog_mode}")
                else catalog_entry.endpointName
            )
            endpoints.append(
                EndpointSpec(
                    endpoint_name,
                    None,
                    self.resolve(catalog_entry.config) if catalog_entry.config else {},
                    None,
                    self.resolve(catalog_entry.result) if catalog_entry.result else None,
                )
            )

        return endpoints


def resolve_spec_from_file(file_path: Path) -> ApiSpec:
    with open(file_path) as f:
        data = json.load(f)

    try:
        spec = ApiSpec(**data)
    except ValidationError as e:
        raise ValueError(f"`{file_path}` is not a valid GDS API spec:\n{e}") from e

    if spec.apiSpecVersion != SUPPORTED_API_SPEC_VERSION:
        raise ValueError(
            f"Unsupported GDS API spec version `{spec.apiSpecVersion}` "
            f"(this client understands version `{SUPPORTED_API_SPEC_VERSION}`). "
            "Update the graphdatascience client."
        )

    return spec
