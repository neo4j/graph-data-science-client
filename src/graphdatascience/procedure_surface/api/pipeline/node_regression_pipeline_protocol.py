from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.model.node_regression_model import NodeRegressionModelV2
from graphdatascience.procedure_surface.api.pipeline.node_regression_metric import NodeRegressionMetric
from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline_results import (
    NodeRegressionPipelineInfoResult,
    NodeRegressionPipelineTrainResult,
)


@runtime_checkable
class NodeRegressionPipelineOps(Protocol):
    def add_node_property(
        self, pipeline_name: str, task_name: str, **config: Any
    ) -> NodeRegressionPipelineInfoResult: ...

    def select_features(
        self, pipeline_name: str, feature_properties: str | list[str]
    ) -> NodeRegressionPipelineInfoResult: ...

    def add_linear_regression(
        self,
        pipeline_name: str,
        *,
        batch_size: int | tuple[int, int] = 100,
        learning_rate: float | tuple[float, float] = 0.001,
        max_epochs: int | tuple[int, int] = 100,
        min_epochs: int | tuple[int, int] = 1,
        patience: int | tuple[int, int] = 1,
        penalty: float | tuple[float, float] = 0.0,
        tolerance: float | tuple[float, float] = 0.001,
    ) -> NodeRegressionPipelineInfoResult: ...

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
    ) -> NodeRegressionPipelineInfoResult: ...

    def configure_split(
        self, pipeline_name: str, *, test_fraction: float = 0.3, validation_folds: int = 3
    ) -> NodeRegressionPipelineInfoResult: ...

    def configure_auto_tuning(
        self, pipeline_name: str, *, max_trials: int = 10
    ) -> NodeRegressionPipelineInfoResult: ...


@runtime_checkable
class NodeRegressionPipelineTrainer(Protocol):
    def train(
        self,
        G: Graph,
        pipeline_name: str,
        *,
        metrics: list[str | NodeRegressionMetric],
        model_name: str,
        target_property: str,
        relationship_types: list[str] = ALL_TYPES,
        target_node_labels: list[str] = ALL_LABELS,
        store_model_to_disk: bool = False,
        random_seed: Any | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> tuple[NodeRegressionModelV2, NodeRegressionPipelineTrainResult]: ...
