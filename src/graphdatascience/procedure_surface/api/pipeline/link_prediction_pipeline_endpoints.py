from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from graphdatascience.procedure_surface.api.link_prediction_predict_endpoints import (
    LinkPredictionPipelinePredictEndpoints,
)
from graphdatascience.procedure_surface.api.pipeline.link_prediction_pipeline import LinkPredictionPipeline
from graphdatascience.procedure_surface.api.pipeline.link_prediction_pipeline_results import (
    LinkPredictionPipelineInfoResult,
)
from graphdatascience.procedure_surface.api.pipeline.link_prediction_train_endpoints import (
    LinkPredictionPipelineTrainEndpoints,
)


class LinkPredictionPipelineEndpoints(ABC):
    @property
    @abstractmethod
    def train(self) -> LinkPredictionPipelineTrainEndpoints:
        """Access training endpoints for link prediction pipelines."""
        pass

    @property
    @abstractmethod
    def predict(self) -> LinkPredictionPipelinePredictEndpoints:
        """Access prediction endpoints for link prediction models trained from this surface."""
        pass

    @abstractmethod
    def create(self, pipeline_name: str) -> tuple[LinkPredictionPipeline, LinkPredictionPipelineInfoResult]:
        """
        Create a new link prediction pipeline.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.

        Returns
        -------
        tuple[LinkPredictionPipeline, LinkPredictionPipelineInfoResult]
            The created pipeline and the corresponding result payload.
        """
        pass

    @abstractmethod
    def get(self, pipeline_name: str) -> LinkPredictionPipeline:
        """
        Retrieve an existing link prediction pipeline by name.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.

        Returns
        -------
        LinkPredictionPipeline
            The reconstructed pipeline object.
        """
        pass

    @abstractmethod
    def add_node_property(
        self, pipeline_name: str, procedure_name: str, **config: Any
    ) -> LinkPredictionPipelineInfoResult:
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
        LinkPredictionPipelineInfoResult
            The updated pipeline state.
        """
        pass

    @abstractmethod
    def add_feature(self, pipeline_name: str, feature_type: str, **config: Any) -> LinkPredictionPipelineInfoResult:
        """
        Add an edge feature step to the pipeline.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.
        feature_type
            Type of feature step to add.
        config
            Additional configuration for the feature step.

        Returns
        -------
        LinkPredictionPipelineInfoResult
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
    ) -> LinkPredictionPipelineInfoResult:
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
        LinkPredictionPipelineInfoResult
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
    ) -> LinkPredictionPipelineInfoResult:
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
        LinkPredictionPipelineInfoResult
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
    ) -> LinkPredictionPipelineInfoResult:
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
        LinkPredictionPipelineInfoResult
            The updated pipeline state.
        """
        pass

    @abstractmethod
    def configure_split(self, pipeline_name: str, **config: Any) -> LinkPredictionPipelineInfoResult:
        """
        Configure the train-test split used by the pipeline.

        Parameters
        ----------
        pipeline_name
            Name of the pipeline.
        config
            Split configuration such as train fraction, test fraction, and validation folds.

        Returns
        -------
        LinkPredictionPipelineInfoResult
            The updated pipeline state.
        """
        pass

    @abstractmethod
    def configure_auto_tuning(self, pipeline_name: str, *, max_trials: int = 10) -> LinkPredictionPipelineInfoResult:
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
        LinkPredictionPipelineInfoResult
            The updated pipeline state.
        """
        pass
