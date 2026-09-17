from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import field_validator

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.utils.gds_datetime import normalize_gds_datetime


class PipelineCatalogEntry(BaseResult):
    pipeline_name: str
    pipeline_type: str
    creation_time: datetime | None = None
    pipeline_info: dict[str, Any] | None = None

    @field_validator("creation_time", mode="before")
    @classmethod
    def parse_creation_time(cls, value: Any) -> Any:
        return normalize_gds_datetime(value)


class PipelineExistsResult(BaseResult):
    pipeline_name: str
    pipeline_type: str
    exists: bool
