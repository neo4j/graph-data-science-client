from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline_endpoints import (
    NodeClassificationPipelineEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline_endpoints import (
    NodeRegressionPipelineEndpoints,
)


class PipelineCatalogEntry(BaseResult):
    pipeline_name: str
    pipeline_type: str
    creation_time: datetime | None = None
    pipeline_info: dict[str, Any] | None = None


class PipelineExistsResult(BaseResult):
    pipeline_name: str
    pipeline_type: str
    exists: bool


class PipelineEndpoints(ABC):
    @abstractmethod
    def list(self, pipeline_name: str | None = None) -> list[PipelineCatalogEntry]:
        """List pipeline catalog entries, optionally filtered by pipeline name."""
        pass

    @abstractmethod
    def exists(self, pipeline_name: str) -> PipelineExistsResult | None:
        """Return pipeline existence details when present, otherwise None."""
        pass

    @abstractmethod
    def drop(self, pipeline_name: str, *, fail_if_missing: bool = False) -> PipelineCatalogEntry | None:
        """Drop a pipeline from the catalog, optionally failing when missing."""
        pass

    @property
    @abstractmethod
    def node_classification(self) -> NodeClassificationPipelineEndpoints:
        """Access node classification pipeline procedures."""
        pass

    @property
    @abstractmethod
    def node_regression(self) -> NodeRegressionPipelineEndpoints:
        """Access node regression pipeline procedures."""
        pass
