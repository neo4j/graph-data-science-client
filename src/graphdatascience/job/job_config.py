import pathlib
from abc import ABC
from typing import Annotated, Any, Literal, Union

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator
from pydantic.alias_generators import to_snake


class JobConfigValidationError(Exception):
    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("Invalid job config:\n" + "\n".join(f"- {error}" for error in errors))


class _JobConfigModel(BaseModel, alias_generator=to_snake, populate_by_name=True):
    pass


class NativeProjection(_JobConfigModel):
    graph: str
    node_labels: list[str] | None = None
    relationship_types: list[str] | None = None
    node_properties: list[str] | None = None
    relationship_properties: list[str] | None = None
    undirected_relationship_types: list[str] | None = None
    inverse_indexed_relationship_types: list[str] | None = None


class CypherProjection(_JobConfigModel):
    graph: str
    query: str


class Projection(_JobConfigModel):
    """A single projection entry, tagged as either ``native`` or ``cypher``."""

    native: NativeProjection | None = None
    cypher: CypherProjection | None = None

    @model_validator(mode="after")
    def _exactly_one_kind(self) -> "Projection":
        if (self.native is None) == (self.cypher is None):
            raise ValueError("a projection must specify exactly one of 'native' or 'cypher'")
        return self

    @property
    def spec(self) -> NativeProjection | CypherProjection:
        return self.native if self.native is not None else self.cypher  # type: ignore[return-value]

    @property
    def graph(self) -> str:
        return self.spec.graph


class WriteOrMutateMode(_JobConfigModel):
    name: Literal["write", "mutate"]
    property: str


class GraphParams(ABC):
    graph: str


class AlgorithmParams(_JobConfigModel, GraphParams):
    algorithm: str
    graph: str
    mode: Literal["stream"] | WriteOrMutateMode
    configuration: dict[str, Any] = Field(default_factory=dict)


class AlgorithmStep(_JobConfigModel):
    name: str
    type: Literal["algorithm"]
    params: AlgorithmParams


class WriteBackParams(_JobConfigModel, GraphParams):
    graph: str
    node_properties: list[str] | None = None
    relationship_properties: list[str] | None = None
    relationship_types: list[str] | None = None


class WriteBackStep(_JobConfigModel):
    name: str
    type: Literal["write-back"]
    params: WriteBackParams


Step = Annotated[Union[AlgorithmStep, WriteBackStep], Field(discriminator="type")]


class JobConfig(_JobConfigModel):
    """
    Typed representation of a job config, matching the fields and types of `job-config.schema.json`.
    """

    projections: list[Projection] = Field(min_length=1)
    steps: list[Step] = Field(min_length=1)

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
