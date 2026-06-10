from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.model.node_regression_model import NodeRegressionModelV2
from graphdatascience.procedure_surface.api.pipeline.node_regression_metric import NodeRegressionMetric
from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline import NodeRegressionPipeline
from graphdatascience.procedure_surface.api.pipeline.node_regression_pipeline_results import (
    NodeRegressionPipelineInfoResult,
    NodeRegressionPipelineTrainResult,
)
from graphdatascience.procedure_surface.api.pipeline.node_regression_predict_endpoints import (
    NodeRegressionPipelinePredictEndpoints,
)


class NodeRegressionPipelineEndpoints(ABC):
    @property
    @abstractmethod
    def predict(self) -> NodeRegressionPipelinePredictEndpoints:
        """Access prediction endpoints for node regression models trained from this surface."""
        pass

    @abstractmethod
    def create(self, pipeline_name: str) -> tuple[NodeRegressionPipeline, NodeRegressionPipelineInfoResult]:
        """
        Create a new node regression pipeline.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.

        Returns
        -------
        tuple[NodeRegressionPipeline, NodeRegressionPipelineInfoResult]
            The created pipeline and the corresponding result payload.
        """
        pass

    @abstractmethod
    def get(self, pipeline_name: str) -> NodeRegressionPipeline:
        """
        Retrieve an existing node regression pipeline by name.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.

        Returns
        -------
        NodeRegressionPipeline
            The reconstructed pipeline object.
        """
        pass

    @abstractmethod
    def add_node_property(self, pipeline_name: str, task_name: str, **config: Any) -> NodeRegressionPipelineInfoResult:
        """
        Add a node property step to the pipeline.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.
        task_name
            The name of the node property step to add.
        config
            Additional configuration for the node property step.

        Returns
        -------
        NodeRegressionPipelineInfoResult
            The updated pipeline state.
        """
        pass

    @abstractmethod
    def select_features(
        self, pipeline_name: str, feature_properties: str | list[str]
    ) -> NodeRegressionPipelineInfoResult:
        """
        Select the node properties used as input features.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.
        feature_properties
            One or more node properties to use as features.

        Returns
        -------
        NodeRegressionPipelineInfoResult
            The updated pipeline state.
        """
        pass

    @abstractmethod
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
    ) -> NodeRegressionPipelineInfoResult:
        """
        Add a linear regression model candidate to the pipeline.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.
        batch_size
            Batch size to use during training. Pass a two-value tuple to define a parameter range.
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
        NodeRegressionPipelineInfoResult
            The updated pipeline state.
        """
        pass

    @abstractmethod
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
    ) -> NodeRegressionPipelineInfoResult:
        """
        Add a random forest model candidate to the pipeline.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.
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
        NodeRegressionPipelineInfoResult
            The updated pipeline state.
        """
        pass

    @abstractmethod
    def configure_split(
        self, pipeline_name: str, *, test_fraction: float = 0.3, validation_folds: int = 3
    ) -> NodeRegressionPipelineInfoResult:
        """
        Configure the train-test split used by the pipeline.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.
        test_fraction
            Fraction of nodes reserved for testing.
        validation_folds
            Number of validation folds to use.

        Returns
        -------
        NodeRegressionPipelineInfoResult
            The updated pipeline state.
        """
        pass

    @abstractmethod
    def configure_auto_tuning(self, pipeline_name: str, *, max_trials: int = 10) -> NodeRegressionPipelineInfoResult:
        """
        Configure auto-tuning for the pipeline.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.
        max_trials
            Maximum number of trials to run during auto-tuning.

        Returns
        -------
        NodeRegressionPipelineInfoResult
            The updated pipeline state.
        """
        pass

    @abstractmethod
    def train(
        self,
        G: GraphV2,
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
    ) -> tuple[NodeRegressionModelV2, NodeRegressionPipelineTrainResult]:
        """
        Train a node regression model from the given pipeline.

        Parameters
        ----------
        G
            Graph object to use
        pipeline_name
            Name of the pipeline.
        metrics
            Metrics to optimize for. Plain strings and ``NodeRegressionMetric`` values are both accepted.
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
        tuple[NodeRegressionModelV2, NodeRegressionPipelineTrainResult]
            The trained model and the corresponding training result.
        """
        pass
