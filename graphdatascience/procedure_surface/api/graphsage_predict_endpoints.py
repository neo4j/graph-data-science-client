from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult

from ...graph.graph_object import Graph


class GraphSagePredictEndpoints(ABC):
    """
    Abstract base class defining the API for the GraphSage algorithm.
    """

    @abstractmethod
    def stream(self, G: Graph, **config: Any) -> DataFrame:
        pass

    @abstractmethod
    def write(self, G: Graph, **config: Any) -> GraphSageWriteResult:
        pass

    @abstractmethod
    def mutate(self, G: Graph, **config: Any) -> GraphSageMutateResult:
        pass

    @abstractmethod
    def estimate(self, G: Graph, **config: Any) -> EstimationResult:
        pass


class GraphSageMutateResult(BaseResult):
    node_count: int
    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    mutate_millis: int
    configuration: dict[str, Any]


class GraphSageWriteResult(BaseResult):
    node_count: int
    node_properties_written: int
    pre_processing_millis: int
    compute_millis: int
    write_millis: int
    configuration: dict[str, Any]
