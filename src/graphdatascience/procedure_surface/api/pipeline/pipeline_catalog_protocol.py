from __future__ import annotations

from typing import Protocol, Sequence

from graphdatascience.procedure_surface.api.pipeline.pipeline_catalog_result import (
    PipelineCatalogEntry,
    PipelineExistsResult,
)


class PipelineCatalogEntryProtocol(Protocol):
    """
    Represents a pipeline in the catalog.
    """

    pipeline_name: str
    pipeline_type: str


class PipelineCatalogProtocol(Protocol):
    def list(self, pipeline_name: str | None = None) -> Sequence[PipelineCatalogEntryProtocol]: ...

    def exists(self, pipeline_name: str) -> PipelineExistsResult | None: ...

    def get(self, pipeline_name: str) -> PipelineCatalogEntry: ...

    def drop(self, pipeline_name: str, *, fail_if_missing: bool = False) -> PipelineCatalogEntryProtocol | None: ...
