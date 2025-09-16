from __future__ import annotations

import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional, Union

import neo4j
from pydantic import BaseModel, Field, field_validator
from pydantic.alias_generators import to_camel


class GraphBackend(ABC):
    @abstractmethod
    def graph_info(self) -> GraphInfo:
        pass

    @abstractmethod
    def exists(self) -> bool:
        pass

    @abstractmethod
    def drop(self, fail_if_missing: bool = True) -> dict[str, Any]:
        pass


class GraphInfo(BaseModel, alias_generator=to_camel):
    graph_name: str
    database: str
    database_location: str
    configuration: dict[str, Any]
    memory_usage: str
    size_in_bytes: int
    node_count: int
    relationship_count: int
    creation_time: datetime
    modification_time: datetime
    graph_schema: dict[str, Any] = Field(alias="schema")
    schema_with_orientation: dict[str, Any]
    degree_distribution: Optional[dict[str, Union[int, float]]] = None
    density: float

    @field_validator("creation_time", "modification_time", mode="before")
    @classmethod
    def strip_timezone(cls, value: Any) -> Any:
        if isinstance(value, str):
            return re.sub(r"\[.*\]$", "", value)
        if isinstance(value, neo4j.time.DateTime):
            return value.to_native()
        return value
