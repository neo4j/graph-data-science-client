from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from graphdatascience.procedure_surface.api.node_classification_predict_endpoints import (
    NodeClassificationPipelinePredictEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline import NodeClassificationPipeline
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline_results import (
    NodeClassificationPipelineInfoResult,
)
from graphdatascience.procedure_surface.api.pipeline.node_classification_train_endpoints import (
    NodeClassificationPipelineTrainEndpoints,
)


class NodeClassificationPipelineEndpoints(ABC):
    @property
    @abstractmethod
    def train(self) -> NodeClassificationPipelineTrainEndpoints:
        """Access training endpoints for node classification pipelines."""
        pass

    @property
    @abstractmethod
    def predict(self) -> NodeClassificationPipelinePredictEndpoints:
        """Access prediction endpoints for node classification models trained from this surface."""
        pass

    @abstractmethod
    def create(self, pipeline_name: str) -> tuple[NodeClassificationPipeline, NodeClassificationPipelineInfoResult]:
        """
        Create a new node classification pipeline.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.

        Returns
        -------
        tuple[NodeClassificationPipeline, NodeClassificationPipelineInfoResult]
            The created pipeline and the corresponding result payload.
        """
        pass

    @abstractmethod
    def get(self, pipeline_name: str) -> NodeClassificationPipeline:
        """
        Retrieve an existing node classification pipeline by name.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.

        Returns
        -------
        NodeClassificationPipeline
            The reconstructed pipeline object.
        """
        pass

    @abstractmethod
    def add_node_property(
        self, pipeline_name: str, procedure_name: str, **config: Any
    ) -> NodeClassificationPipelineInfoResult:
        """
        Add a node property step to the pipeline.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.
        procedure_name
            The procedure name of the node property step to add.
        config
            Additional configuration for the node property step.

        Returns
        -------
        NodeClassificationPipelineInfoResult
            The updated pipeline state.
        """
        pass

    @abstractmethod
    def select_features(
        self, pipeline_name: str, node_properties: str | list[str]
    ) -> NodeClassificationPipelineInfoResult:
        """
        Select the node properties used as input features.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.
        node_properties
            One or more node properties to use as features.

        Returns
        -------
        NodeClassificationPipelineInfoResult
            The updated pipeline state.
        """
        pass

    @abstractmethod
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
    ) -> NodeClassificationPipelineInfoResult:
        """
        Add a logistic regression model candidate to the pipeline.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.
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
        pass

    @abstractmethod
    def add_random_forest(
        self,
        pipeline_name: str,
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
        pipeline_name
            Name of the pipeline.
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
        pass

    @abstractmethod
    def add_mlp(
        self,
        pipeline_name: str,
        *,
        hidden_layer_sizes: list[int],
        penalty: float | tuple[float, float] = 0.0,
    ) -> NodeClassificationPipelineInfoResult:
        """
        Add a multi-layer perceptron model candidate to the pipeline.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.
        hidden_layer_sizes
            Sizes of the hidden layers in the neural network.
        penalty
            Penalty term to use during training. Pass a two-value tuple to define a parameter range.

        Returns
        -------
        NodeClassificationPipelineInfoResult
            The updated pipeline state.
        """
        pass

    @abstractmethod
    def configure_split(
        self, pipeline_name: str, *, test_fraction: float = 0.3, validation_folds: int = 3
    ) -> NodeClassificationPipelineInfoResult:
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
        NodeClassificationPipelineInfoResult
            The updated pipeline state.
        """
        pass

    @abstractmethod
    def configure_auto_tuning(
        self, pipeline_name: str, *, max_trials: int = 10
    ) -> NodeClassificationPipelineInfoResult:
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
        NodeClassificationPipelineInfoResult
            The updated pipeline state.
        """
        pass
