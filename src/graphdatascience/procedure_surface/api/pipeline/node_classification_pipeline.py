from __future__ import annotations

from typing import Any

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.model.node_classification_model import NodeClassificationModel
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline_protocol import (
    NodeClassificationPipelineOps,
    NodeClassificationPipelineTrainer,
)
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline_results import (
    NodeClassificationPipelineInfoResult,
    NodeClassificationPipelineTrainResult,
)
from graphdatascience.procedure_surface.api.pipeline.pipeline_catalog_protocol import (
    PipelineCatalogEntryProtocol,
    PipelineCatalogProtocol,
)


class NodeClassificationPipeline:
    """
    Represents a node classification training pipeline.

    Construct this using :func:`gds.v2.pipeline.node_classification.create()`.
    """

    def __init__(
        self,
        name: str,
        ops: NodeClassificationPipelineOps,
        trainer: NodeClassificationPipelineTrainer,
        catalog: PipelineCatalogProtocol,
    ) -> None:
        self._name = name
        self._ops = ops
        self._trainer = trainer
        self._catalog = catalog

    def name(self) -> str:
        """Return the pipeline name."""
        return self._name

    def add_node_property(self, task_name: str, **config: Any) -> NodeClassificationPipelineInfoResult:
        """
        Add a node property step to the pipeline.

        Parameters
        ----------
        task_name
            The task name of the node property step to add.

        Returns
        -------
        NodeClassificationPipelineInfoResult
            The updated pipeline state.
        """
        return self._ops.add_node_property(self._name, task_name, **config)

    def select_features(self, node_properties: str | list[str]) -> NodeClassificationPipelineInfoResult:
        """
        Select the node properties used as input features.

        Parameters
        ----------
        node_properties
            One or more node properties to use as features.

        Returns
        -------
        NodeClassificationPipelineInfoResult
            The updated pipeline state.
        """
        return self._ops.select_features(self._name, node_properties)

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
    ) -> NodeClassificationPipelineInfoResult:
        """
        Add a logistic regression model candidate to the pipeline.

        Parameters
        ----------
        batch_size
            Batch size to use during training. Pass a two-value tuple to define a parameter range.
        class_weights
            Optional class weights to use during training.
        focus_weight
            Focus weight for optimization. Pass a two-value tuple to define a parameter range.
        learning_rate
            Learning rate for optimization. Pass a two-value tuple to define a parameter range.
        max_epochs
            Maximum number of training epochs. Pass a two-value tuple to define a parameter range.
        min_epochs
            Minimum number of training epochs. Pass a two-value tuple to define a parameter range.
        patience
            Early stopping patience. Pass a two-value tuple to define a parameter range.
        penalty
            Penalty term to use during training. Pass a two-value tuple to define a parameter range.
        tolerance
            Convergence tolerance. Pass a two-value tuple to define a parameter range.

        Returns
        -------
        NodeClassificationPipelineInfoResult
            The updated pipeline state.
        """
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
    ) -> NodeClassificationPipelineInfoResult:
        """
        Add a random forest model candidate to the pipeline.

        Parameters
        ----------
        criterion
            Split criterion to optimize.
        max_depth
            Maximum tree depth. Pass a two-value tuple to define a parameter range.
        max_features_ratio
            Fraction of features sampled per split. Pass a two-value tuple to define a parameter range.
        min_leaf_size
            Minimum number of samples in a leaf. Pass a two-value tuple to define a parameter range.
        min_split_size
            Minimum number of samples required to split a node. Pass a two-value tuple to define a parameter range.
        number_of_decision_trees
            Number of trees to train. Pass a two-value tuple to define a parameter range.
        number_of_samples_ratio
            Fraction of samples used per tree. Pass a two-value tuple to define a parameter range.

        Returns
        -------
        NodeClassificationPipelineInfoResult
            The updated pipeline state.
        """
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
    ) -> NodeClassificationPipelineInfoResult:
        """
        Add a multi-layer perceptron model candidate to the pipeline.

        Parameters
        ----------
        batch_size
            Batch size to use during training. Pass a two-value tuple to define a parameter range.
        class_weights
            Optional class weights to use during training.
        focus_weight
            Focus weight for optimization. Pass a two-value tuple to define a parameter range.
        hidden_layer_sizes
            Sizes of the hidden layers in the neural network.
        learning_rate
            Learning rate for optimization. Pass a two-value tuple to define a parameter range.
        max_epochs
            Maximum number of training epochs. Pass a two-value tuple to define a parameter range.
        min_epochs
            Minimum number of training epochs. Pass a two-value tuple to define a parameter range.
        patience
            Early stopping patience. Pass a two-value tuple to define a parameter range.
        penalty
            Penalty term to use during training. Pass a two-value tuple to define a parameter range.
        tolerance
            Convergence tolerance. Pass a two-value tuple to define a parameter range.

        Returns
        -------
        NodeClassificationPipelineInfoResult
            The updated pipeline state.
        """
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
        self, *, test_fraction: float = 0.3, validation_folds: int = 3
    ) -> NodeClassificationPipelineInfoResult:
        """
        Configure the train-test split used by the pipeline.

        Parameters
        ----------
        test_fraction
            Fraction of nodes reserved for testing.
        validation_folds
            Number of validation folds to use.

        Returns
        -------
        NodeClassificationPipelineInfoResult
            The updated pipeline state.
        """
        return self._ops.configure_split(self._name, test_fraction=test_fraction, validation_folds=validation_folds)

    def configure_auto_tuning(self, *, max_trials: int = 10) -> NodeClassificationPipelineInfoResult:
        """
        Configure auto-tuning for the pipeline.

        Parameters
        ----------
        max_trials
            Maximum number of trials to run during auto-tuning.

        Returns
        -------
        NodeClassificationPipelineInfoResult
            The updated pipeline state.
        """
        return self._ops.configure_auto_tuning(self._name, max_trials=max_trials)

    def exists(self) -> bool:
        """Return whether the pipeline exists."""
        return self._catalog.exists(self._name) is not None

    def drop(self, fail_if_missing: bool = False) -> PipelineCatalogEntryProtocol | None:
        """Drop the pipeline and return its catalog entry when available."""
        return self._catalog.drop(self._name, fail_if_missing=fail_if_missing)

    def train(
        self,
        G: Graph,
        *,
        metrics: list[str],
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
    ) -> tuple[NodeClassificationModel, NodeClassificationPipelineTrainResult]:
        """
        Train a node classification model from this pipeline.

        Parameters
        ----------
        G
            Graph object to use
        metrics
            Metrics to optimize for.
        model_name
            Name of the trained model.
        target_property
            The target node property to predict.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        target_node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        store_model_to_disk
            Whether to persist the trained model to disk.
        random_seed
            Seed for random number generation to ensure reproducible results.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        log_progress
            Display progress logging.
        sudo
            Disable the memory guard.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

        Returns
        -------
        tuple[NodeClassificationModel, NodeClassificationPipelineTrainResult]
            The trained model and the corresponding training result.
        """
        return self._trainer.train(
            G,
            self._name,
            metrics=metrics,
            model_name=model_name,
            target_property=target_property,
            relationship_types=relationship_types,
            target_node_labels=target_node_labels,
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
        G: Graph,
        *,
        metrics: list[str],
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
    ) -> EstimationResult:
        """
        Estimate the memory required to train a node classification model from this pipeline.

        Parameters
        ----------
        G
            Graph object to use
        metrics
            Metrics to optimize for.
        model_name
            Name of the trained model.
        target_property
            The target node property to predict.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        target_node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        store_model_to_disk
            Whether to persist the trained model to disk.
        random_seed
            Seed for random number generation to ensure reproducible results.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        log_progress
            Display progress logging.
        sudo
            Disable the memory guard.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

        Returns
        -------
        EstimationResult
            The estimated memory footprint for training.
        """
        return self._trainer.train.estimate(
            G,
            self._name,
            metrics=metrics,
            model_name=model_name,
            target_property=target_property,
            relationship_types=relationship_types,
            target_node_labels=target_node_labels,
            store_model_to_disk=store_model_to_disk,
            random_seed=random_seed,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )
