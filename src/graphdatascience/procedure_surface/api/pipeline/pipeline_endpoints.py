from __future__ import annotations

from abc import ABC, abstractmethod

from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline_endpoints import (
    NodeRegressionPipelineEndpoints,
)


class PipelineEndpoints(ABC):
    @property
    @abstractmethod
    def node_regression(self) -> NodeRegressionPipelineEndpoints:
        """Access node regression pipeline procedures."""
        pass
