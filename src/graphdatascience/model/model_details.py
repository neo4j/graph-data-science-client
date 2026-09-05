import datetime
from typing import Any

from pydantic import BaseModel, field_validator
from pydantic.alias_generators import to_camel

from graphdatascience.utils.gds_datetime import normalize_gds_datetime


class ModelDetails(BaseModel, alias_generator=to_camel):
    model_name: str
    model_type: str
    train_config: dict[str, Any]
    graph_schema: dict[str, Any]
    loaded: bool
    stored: bool
    published: bool
    model_info: dict[str, Any]
    creation_time: datetime.datetime

    @field_validator("creation_time", mode="before")
    @classmethod
    def parse_creation_time(cls, value: Any) -> Any:
        return normalize_gds_datetime(value)

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)
