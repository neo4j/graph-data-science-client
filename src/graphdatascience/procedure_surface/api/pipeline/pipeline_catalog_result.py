from __future__ import annotations

from datetime import datetime
from typing import Any

from graphdatascience.procedure_surface.api.base_result import BaseResult


class PipelineCatalogEntry(BaseResult):
    pipeline_name: str
    pipeline_type: str
    creation_time: datetime | None = None
    pipeline_info: dict[str, Any] | None = None


class PipelineExistsResult(BaseResult):
    pipeline_name: str
    pipeline_type: str
    exists: bool
