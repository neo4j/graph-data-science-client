from __future__ import annotations

from abc import ABC, abstractmethod

from graphdatascience.procedure_surface.api.embedding.encode_endpoints import EncodeEndpoints
from graphdatascience.procedure_surface.api.embedding.predict_endpoints import PredictEndpoints
from graphdatascience.procedure_surface.api.embedding.train_endpoints import TrainEndpoints

__all__ = ["EmbeddingsEndpoints"]


class EmbeddingsEndpoints(ABC):
    @property
    @abstractmethod
    def train(self) -> TrainEndpoints:
        """Access embedding training procedures."""
        pass

    @property
    @abstractmethod
    def predict(self) -> PredictEndpoints:
        """Access embedding prediction procedures."""
        pass

    @property
    @abstractmethod
    def encode(self) -> EncodeEndpoints:
        """Access embedding encoding procedures."""
        pass
