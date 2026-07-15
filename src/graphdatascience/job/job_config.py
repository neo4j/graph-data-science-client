import pathlib
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, ValidationError
from pydantic.alias_generators import to_camel


class JobConfigValidationError(Exception):
    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("Invalid job config:\n" + "\n".join(f"- {error}" for error in errors))


class _JobConfigModel(BaseModel, alias_generator=to_camel, populate_by_name=True):
    pass


class CypherProjection(_JobConfigModel):
    query: str

class NativeProjection(_JobConfigModel):
    name: str
    node_labels: list[Any] | None = None
    relationship_types: list[Any] | None = None
    node_properties: list[Any] | None = None
    relationship_properties: list[Any] | None = None
    undirected_relationship_types: list[Any] | None = None
    inverse_indexed_relationship_types: list[Any] | None = None


class JobMetadata(_JobConfigModel):
    name: str
    aura_instance_id: str
    version: str


class JobSchedule(_JobConfigModel):
    expression: str | None = None


class WriteOrMutateMode(_JobConfigModel):
    name: Literal["write", "mutate"]
    property: str


class Algorithm(_JobConfigModel):
    name: str | None = None
    mode: Literal["stream"] | WriteOrMutateMode
    config: dict[str, Any] = Field(default_factory=dict)


class JobConfig(_JobConfigModel):
    """
    Typed representation of a job config, matching the fields and types of `job-config.schema.json`.
    """

    metadata: JobMetadata
    schedule: JobSchedule | None = None
    projections: list[CypherProjection | NativeProjection] = Field(min_length=1)
    algorithms: list[Algorithm] = Field(min_length=1)

    @classmethod
    def from_yaml_file(cls, path: str | pathlib.Path) -> "JobConfig":
        with pathlib.Path(path).open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        try:
            return cls.model_validate(data)
        except ValidationError as e:
            messages = [
                f"{'.'.join(str(part) for part in error['loc']) or '<root>'}: {error['msg']}" for error in e.errors()
            ]
            raise JobConfigValidationError(messages) from e
