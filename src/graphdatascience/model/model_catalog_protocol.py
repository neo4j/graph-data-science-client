from __future__ import annotations

from typing import Protocol

from graphdatascience.model.model_details import ModelDetails
from graphdatascience.procedure_surface.api.model.model_catalog_endpoints import (
    ModelDeleteResult,
    ModelExistsResult,
    ModelLoadResult,
    ModelStoreResult,
)


class ModelCatalogProtocol(Protocol):
    """The subset of model catalog operations a :class:`Model` object delegates to."""

    def get(self, model_name: str) -> ModelDetails: ...

    def exists(self, model_name: str) -> ModelExistsResult | None: ...

    def drop(self, model_name: str, *, fail_if_missing: bool = True) -> ModelDetails | None: ...

    def delete(self, model_name: str, fail_if_missing: bool = False) -> ModelDeleteResult | None: ...

    def load(self, model_name: str) -> ModelLoadResult: ...

    def store(self, model_name: str, *, fail_if_unsupported: bool = False) -> ModelStoreResult: ...

    def publish(self, model_name: str) -> ModelDetails: ...
