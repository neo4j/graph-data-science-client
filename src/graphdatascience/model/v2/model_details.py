import datetime
from typing import Any

from pydantic import BaseModel, Field
from pydantic.alias_generators import to_camel


class ModelDetails(BaseModel, alias_generator=to_camel):
    name: str = Field(alias="modelName")
    type: str = Field(alias="modelType")
    train_config: dict[str, Any]
    graph_schema: dict[str, Any]
    loaded: bool
    stored: bool
    published: bool
    model_info: dict[str, Any]  # TODO better typing in actual model?
    creation_time: datetime.datetime

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)
