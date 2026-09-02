from typing import Any

from pydantic import BaseModel
from pydantic.alias_generators import to_camel


class BaseResult(BaseModel, alias_generator=to_camel, populate_by_name=True):
    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)


class StatsResult(BaseResult):
    compute_millis: int
    pre_processing_millis: int
    configuration: dict[str, Any]


class MutateResult(StatsResult):
    mutate_millis: int


class WriteResult(StatsResult):
    write_millis: int


class NodeResult(BaseResult):
    node_properties_written: int
