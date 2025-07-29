from __future__ import annotations

import re
from datetime import datetime
from abc import ABC
from typing import Optional, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic.alias_generators import to_camel

from graphdatascience import Graph


class CatalogEndpoints(ABC):

    def list(self, G: Optional[Graph] = None) -> GraphListResult:
        pass



class GraphListResult(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

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
    graph_schema: dict[str, Any] = Field(alias='schema')
    schema_with_orientation: dict[str, Any]
    degree_distribution: dict[str, Any]

    @field_validator("creation_time", "modification_time", mode="before")
    @classmethod
    def strip_timezone(cls, value):
        if isinstance(value, str):
            return re.sub(r'\[.*\]$', '', value)
        return value
