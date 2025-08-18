import datetime
from typing import Any
from pydantic import BaseModel
from abc import ABC, abstractmethod
from pydantic.alias_generators import to_camel


class ModelInfo(BaseModel, alias_generator=to_camel):
    name: str
    type: str
    train_config: dict[str, Any]
    graph_schema: dict[str, Any]
    loaded: bool
    stored: bool
    shared: bool
    published: bool
    model_info: dict[str, Any]  # TODO better typing in actual model?
    creation_time: datetime.datetime  # TODO correct type? / conversion needed

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)
