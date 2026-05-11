from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from graphdatascience.procedure_surface.api.pipeline.link_prediction_pipeline_results import (
    LinkPredictionPipelineInfoResult,
)
from graphdatascience.procedure_surface.api.pipeline.link_prediction_train_endpoints import (
    LinkPredictionPipelineTrainEndpoints,
)


@runtime_checkable
class LinkPredictionPipelineOps(Protocol):
    def add_node_property(
        self, pipeline_name: str, procedure_name: str, **config: Any
    ) -> LinkPredictionPipelineInfoResult: ...

    def add_feature(self, pipeline_name: str, feature_type: str, **config: Any) -> LinkPredictionPipelineInfoResult: ...

    def add_logistic_regression(
        self,
        pipeline_name: str,
        *,
        batch_size: int | tuple[int, int] = 100,
        class_weights: list[float] | None = None,
        focus_weight: float | tuple[float, float] = 0.0,
        learning_rate: float | tuple[float, float] = 0.001,
        max_epochs: int | tuple[int, int] = 100,
        min_epochs: int | tuple[int, int] = 1,
        patience: int | tuple[int, int] = 1,
        penalty: float | tuple[float, float] = 0.0,
        tolerance: float | tuple[float, float] = 0.001,
    ) -> LinkPredictionPipelineInfoResult: ...

    def add_random_forest(
        self,
        pipeline_name: str,
        *,
        max_depth: int | tuple[int, int] = 2147483647,
        max_features_ratio: float | tuple[float, float] | None = None,
        min_leaf_size: int | tuple[int, int] = 1,
        min_split_size: int | tuple[int, int] = 2,
        number_of_decision_trees: int | tuple[int, int] = 100,
        number_of_samples_ratio: float | tuple[float, float] = 1.0,
    ) -> LinkPredictionPipelineInfoResult: ...

    def add_mlp(
        self,
        pipeline_name: str,
        *,
        hidden_layer_sizes: list[int],
        penalty: float | tuple[float, float] = 0.0,
    ) -> LinkPredictionPipelineInfoResult: ...

    def configure_split(self, pipeline_name: str, **config: Any) -> LinkPredictionPipelineInfoResult: ...

    def configure_auto_tuning(
        self, pipeline_name: str, *, max_trials: int = 10
    ) -> LinkPredictionPipelineInfoResult: ...


@runtime_checkable
class LinkPredictionPipelineTrainer(Protocol):
    @property
    def train(self) -> LinkPredictionPipelineTrainEndpoints: ...
