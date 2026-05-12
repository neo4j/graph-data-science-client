from __future__ import annotations

from typing import Any

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABEL
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.model.link_prediction_model import LinkPredictionModelV2
from graphdatascience.procedure_surface.api.pipeline.link_prediction_pipeline_protocol import (
    LinkPredictionPipelineOps,
    LinkPredictionPipelineTrainer,
)
from graphdatascience.procedure_surface.api.pipeline.link_prediction_pipeline_results import (
    LinkPredictionPipelineInfoResult,
    LinkPredictionPipelineTrainResult,
)
from graphdatascience.procedure_surface.api.pipeline.pipeline_catalog_protocol import (
    PipelineCatalogEntryProtocol,
    PipelineCatalogProtocol,
)


class LinkPredictionPipeline:
    """
    Represents a link prediction training pipeline.

    Construct this using :func:`gds.v2.pipeline.link_prediction.create()`.
    """

    def __init__(
        self,
        name: str,
        ops: LinkPredictionPipelineOps,
        trainer: LinkPredictionPipelineTrainer,
        catalog: PipelineCatalogProtocol,
    ) -> None:
        self._name = name
        self._ops = ops
        self._trainer = trainer
        self._catalog = catalog

    def name(self) -> str:
        return self._name

    def add_node_property(self, task_name: str, **config: Any) -> LinkPredictionPipelineInfoResult:
        return self._ops.add_node_property(self._name, task_name, **config)

    def add_feature(self, feature_type: str, *, node_properties: list[str]) -> LinkPredictionPipelineInfoResult:
        return self._ops.add_feature(self._name, feature_type, node_properties=node_properties)

    def add_logistic_regression(
        self,
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
    ) -> LinkPredictionPipelineInfoResult:
        return self._ops.add_logistic_regression(
            self._name,
            batch_size=batch_size,
            class_weights=class_weights,
            focus_weight=focus_weight,
            learning_rate=learning_rate,
            max_epochs=max_epochs,
            min_epochs=min_epochs,
            patience=patience,
            penalty=penalty,
            tolerance=tolerance,
        )

    def add_random_forest(
        self,
        *,
        criterion: str | None = "GINI",
        max_depth: int | tuple[int, int] = 2147483647,
        max_features_ratio: float | tuple[float, float] | None = None,
        min_leaf_size: int | tuple[int, int] = 1,
        min_split_size: int | tuple[int, int] = 2,
        number_of_decision_trees: int | tuple[int, int] = 100,
        number_of_samples_ratio: float | tuple[float, float] = 1.0,
    ) -> LinkPredictionPipelineInfoResult:
        return self._ops.add_random_forest(
            self._name,
            criterion=criterion,
            max_depth=max_depth,
            max_features_ratio=max_features_ratio,
            min_leaf_size=min_leaf_size,
            min_split_size=min_split_size,
            number_of_decision_trees=number_of_decision_trees,
            number_of_samples_ratio=number_of_samples_ratio,
        )

    def add_mlp(
        self,
        *,
        batch_size: int | tuple[int, int] = 100,
        class_weights: list[float] | None = None,
        focus_weight: float | tuple[float, float] = 0.0,
        hidden_layer_sizes: list[int] = [100],
        learning_rate: float | tuple[float, float] = 0.001,
        max_epochs: int | tuple[int, int] = 100,
        min_epochs: int | tuple[int, int] = 1,
        patience: int | tuple[int, int] = 1,
        penalty: float | tuple[float, float] = 0.0,
        tolerance: float | tuple[float, float] = 0.001,
    ) -> LinkPredictionPipelineInfoResult:
        return self._ops.add_mlp(
            self._name,
            batch_size=batch_size,
            class_weights=class_weights,
            focus_weight=focus_weight,
            hidden_layer_sizes=hidden_layer_sizes,
            learning_rate=learning_rate,
            max_epochs=max_epochs,
            min_epochs=min_epochs,
            patience=patience,
            penalty=penalty,
            tolerance=tolerance,
        )

    def configure_split(
        self,
        *,
        negative_relationship_type: str | None = None,
        negative_sampling_ratio: float = 1.0,
        test_fraction: float = 0.1,
        train_fraction: float = 0.1,
        validation_folds: int = 3,
    ) -> LinkPredictionPipelineInfoResult:
        return self._ops.configure_split(
            self._name,
            negative_relationship_type=negative_relationship_type,
            negative_sampling_ratio=negative_sampling_ratio,
            test_fraction=test_fraction,
            train_fraction=train_fraction,
            validation_folds=validation_folds,
        )

    def configure_auto_tuning(self, *, max_trials: int = 10) -> LinkPredictionPipelineInfoResult:
        return self._ops.configure_auto_tuning(self._name, max_trials=max_trials)

    def exists(self) -> bool:
        return self._catalog.exists(self._name) is not None

    def drop(self, failIfMissing: bool = False) -> PipelineCatalogEntryProtocol | None:
        return self._catalog.drop(self._name, fail_if_missing=failIfMissing)

    def train(
        self,
        G: GraphV2,
        *,
        model_name: str,
        metrics: list[str] = ["AUCPR"],
        negative_class_weight: float = 1.0,
        source_node_label: str = ALL_LABEL,
        target_node_label: str = ALL_LABEL,
        target_relationship_type: str,
        store_model_to_disk: bool = False,
        random_seed: Any | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> tuple[LinkPredictionModelV2, LinkPredictionPipelineTrainResult]:
        return self._trainer.train(
            G,
            self._name,
            model_name=model_name,
            metrics=metrics,
            negative_class_weight=negative_class_weight,
            source_node_label=source_node_label,
            target_node_label=target_node_label,
            target_relationship_type=target_relationship_type,
            store_model_to_disk=store_model_to_disk,
            random_seed=random_seed,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )

    def train_estimate(
        self,
        G: GraphV2,
        *,
        model_name: str,
        metrics: list[str] = ["AUCPR"],
        negative_class_weight: float = 1.0,
        source_node_label: str = ALL_LABEL,
        target_node_label: str = ALL_LABEL,
        target_relationship_type: str,
        store_model_to_disk: bool = False,
        random_seed: Any | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> EstimationResult:
        return self._trainer.train.estimate(
            G,
            self._name,
            model_name=model_name,
            metrics=metrics,
            negative_class_weight=negative_class_weight,
            source_node_label=source_node_label,
            target_node_label=target_node_label,
            target_relationship_type=target_relationship_type,
            store_model_to_disk=store_model_to_disk,
            random_seed=random_seed,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )
