from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class EstimationResult(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    node_count: int
    relationship_count: int
    required_memory: str
    tree_view: str
    map_view: dict[str, Any]
    bytes_min: int
    bytes_max: int
    heap_percentage_min: float
    heap_percentage_max: float

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)

    @staticmethod
    def from_cypher(cypher_result: dict[str, Any]) -> EstimationResult:
        return EstimationResult(**cypher_result)
