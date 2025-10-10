from __future__ import annotations

import re
from datetime import datetime
from typing import Any

import neo4j
from pydantic import Field, field_validator

from graphdatascience.procedure_surface.api.base_result import BaseResult


class GraphInfo(BaseResult):
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
    graph_schema: dict[str, Any] = Field(alias="schemaWithOrientation")
    density: float

    @field_validator("creation_time", "modification_time", mode="before")
    @classmethod
    def strip_timezone(cls, value: Any) -> Any:
        if isinstance(value, str):
            return re.sub(r"\[.*\]$", "", value)
        if isinstance(value, neo4j.time.DateTime):
            return value.to_native()
        return value


class GraphInfoWithDegrees(GraphInfo):
    degree_distribution: dict[str, float | int]
