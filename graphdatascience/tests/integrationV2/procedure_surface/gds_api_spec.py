import json
from pathlib import Path
from typing import Any, List, Optional

from pydantic import BaseModel, Field


class TypeInfo(BaseModel, extra="forbid", populate_by_name=True):
    """Represents type information for a parameter or return field."""

    typeName: str = Field(alias="typeName")
    optional: bool = Field(alias="optional")


class Parameter(BaseModel, extra="forbid", populate_by_name=True):
    """Represents a procedure parameter."""

    name: str
    type: TypeInfo
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


class EndpointWithModesSpec(BaseModel, extra="forbid", populate_by_name=True):
    """Represents a GDS procedure with its parameters and modes."""

    name: str
    parameters: List[Parameter]
    modes: List[Mode]

    def callable_modes(self) -> List[EndpointSpec]:
        return [
            EndpointSpec(
                name=f"{self.name}.{mode.mode}",
                parameters=self.parameters + mode.parameters,
                returnFields=mode.returnFields,
            )
            for mode in self.modes
        ]


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
