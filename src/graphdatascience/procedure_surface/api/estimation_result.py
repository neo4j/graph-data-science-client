from __future__ import annotations

from collections.abc import Hashable
from typing import Any

from graphdatascience.procedure_surface.api.base_result import BaseResult


class EstimationResult(BaseResult):
    node_count: int
    relationship_count: int
    required_memory: str
    tree_view: str
    map_view: dict[str, Any]
    bytes_min: int
    bytes_max: int
    heap_percentage_min: float
    heap_percentage_max: float

    @staticmethod
    def from_cypher(cypher_result: dict[Hashable, Any]) -> EstimationResult:
        return EstimationResult.model_validate(cypher_result)
