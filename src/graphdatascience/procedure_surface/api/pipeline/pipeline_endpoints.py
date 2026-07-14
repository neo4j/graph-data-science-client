from __future__ import annotations

from abc import ABC, abstractmethod

from graphdatascience.procedure_surface.api.pipeline.link_prediction_pipeline_endpoints import (
    LinkPredictionPipelineEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline_endpoints import (
    NodeClassificationPipelineEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline_endpoints import (
    NodeRegressionPipelineEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.pipeline_catalog_result import PipelineCatalogEntry

__all__ = [
    "PipelineCatalogEntry",
    "PipelineEndpoints",
]


class PipelineEndpoints(ABC):
    @abstractmethod
    def list(self, pipeline_name: str | None = None) -> list[PipelineCatalogEntry]:
        """List pipeline catalog entries, optionally filtered by pipeline name."""
        pass

    @abstractmethod
    def exists(self, pipeline_name: str) -> bool:
        """Return whether a pipeline with the given name exists in the catalog."""
        pass

    @abstractmethod
    def get(self, pipeline_name: str) -> PipelineCatalogEntry:
        """Return the catalog entry for the given pipeline, raising if it does not exist."""
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
    def link_prediction(self) -> LinkPredictionPipelineEndpoints:
        """Access link prediction pipeline procedures."""
        pass

    @property
    @abstractmethod
    def node_regression(self) -> NodeRegressionPipelineEndpoints:
        """Access node regression pipeline procedures."""
        pass
