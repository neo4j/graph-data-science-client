from __future__ import annotations

from typing import Protocol, Sequence


class PipelineCatalogEntryProtocol(Protocol):
    pipeline_name: str
    pipeline_type: str


class PipelineCatalogProtocol(Protocol):
    def list(self, pipeline_name: str | None = None) -> Sequence[PipelineCatalogEntryProtocol]: ...

    def exists(self, pipeline_name: str) -> PipelineCatalogEntryProtocol | None: ...

    def drop(self, pipeline_name: str, *, fail_if_missing: bool = False) -> PipelineCatalogEntryProtocol | None: ...
