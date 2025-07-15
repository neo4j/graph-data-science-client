from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, repr=True)
class EstimationResult:
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
