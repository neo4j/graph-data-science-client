from typing import Any

from pydantic import BaseModel
from pydantic.alias_generators import to_camel


class BaseResult(BaseModel, alias_generator=to_camel):
    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)
