import json
from enum import Enum
from pathlib import Path
from typing import Any, List, Optional

from pydantic import BaseModel, Field


class SourceKind(Enum):
    POSITIONAL = "POSITIONAL"
    CONFIG = "CONFIG"


class TypeInfo(BaseModel, extra="forbid", populate_by_name=True):
    """Represents type information for a parameter or return field."""

    typeName: str = Field(alias="typeName")
    optional: bool = Field(alias="optional")


class Parameter(BaseModel, extra="forbid", populate_by_name=True):
    """Represents a procedure parameter."""

    name: str
    type: TypeInfo
    sourceKind: SourceKind
    positionIndex: int | None = None
    defaultValue: Optional[Any] = None


class ReturnField(BaseModel, extra="forbid", populate_by_name=True):
    """Represents a return field from a procedure mode."""

    name: str
    type: TypeInfo


class Mode(BaseModel, extra="forbid", populate_by_name=True):
    """Represents an execution mode (stream, stats, mutate, write) for a procedure."""

    mode: str
    parameters: List[Parameter]
    returnFields: List[ReturnField]


class EndpointSpec(BaseModel, extra="forbid", populate_by_name=True):
    name: str
    parameters: List[Parameter]
    returnFields: List[ReturnField]

    def config_parameters(self) -> List[Parameter]:
        return [param for param in self.parameters if param.sourceKind == SourceKind.CONFIG]

    def positional_parameters(self) -> List[Parameter]:
        return [param for param in self.parameters if param.sourceKind == SourceKind.POSITIONAL]


class EndpointWithModesSpec(BaseModel, extra="forbid", populate_by_name=True):
    """Represents a GDS procedure with its parameters and modes."""

    name: str | None = None  # None if only exposed in the Arrow surface and not in the Cypher surface.
    arrowEndpoint: str | None = None  # The endpoint name for the Arrow surface, if applicable.
    parameters: List[Parameter]
    modes: List[Mode]

    def callable_modes(self) -> List[EndpointSpec]:
        base_name = self.name if self.name is not None else self._arrow_base_name()
        return [
            EndpointSpec(
                name=f"{base_name}.{mode.mode}" if mode.mode.lower() != "unknown" else base_name,
                parameters=self.parameters + mode.parameters,
                returnFields=mode.returnFields,
            )
            for mode in self.modes
        ]

    def arrow_only(self) -> bool:
        return self.name is None and self.arrowEndpoint is not None

    def _arrow_base_name(self) -> str:
        # For Arrow-only endpoints derive a base name from the Arrow endpoint, dropping the
        # `v2/` prefix and the leading category, e.g. "v2/embeddings.fastPath" -> "fastPath".
        assert self.arrowEndpoint is not None
        return self.arrowEndpoint.removeprefix("v2/").split(".", 1)[-1]


def resolve_spec_from_file(file_path: Path) -> list[EndpointWithModesSpec]:
    """
    Load and parse the gds-api-spec.json file.

    Args:
        file_path: Path to the gds-api-spec.json file.
                   If None, uses the default location in the repository root.

    Returns:
        GdsApiSpec: Parsed API specification containing all procedures.
    """
    with open(file_path, "r") as f:
        data = json.load(f)

    # The JSON file is a list of procedures at the root level
    procedures = [EndpointWithModesSpec(**proc) for proc in data]

    return procedures
