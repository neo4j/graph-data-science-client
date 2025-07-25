from typing import Any

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class ArrowBaseModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    def dump_camel(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True)

    def dump_json(self) -> str:
        return self.model_dump_json(by_alias=True)
