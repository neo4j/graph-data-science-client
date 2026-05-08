from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.model.node_classification_model import NodeClassificationModelV2
from graphdatascience.procedure_surface.api.pipeline.node_classification_pipeline_results import (
    NodeClassificationPipelineTrainResult,
)


class NodeClassificationPipelineTrainEndpoints(ABC):
    @abstractmethod
    def __call__(
        self,
        G: GraphV2,
        pipeline_name: str,
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
    ) -> tuple[NodeClassificationModelV2, NodeClassificationPipelineTrainResult]:
        """
        Train a node classification model from the specified pipeline.

        Parameters
        ----------
        G
            Graph object to use.
        pipeline_name
            Name of the pipeline.
        metrics
            Metrics to optimize for.
        model_name
            Name of the trained model.
        target_property
            Target node property to predict.
        relationship_types
            Relationship type filter.
        target_node_labels
            Node label filter.
        store_model_to_disk
            Whether to persist the trained model to disk.
        random_seed
            Seed for random number generation.
        username
            Optional impersonated user.
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
        tuple[NodeClassificationModelV2, NodeClassificationPipelineTrainResult]
            Trained model and training result.
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2,
        pipeline_name: str,
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
        Estimate memory required to train a node classification model.

        Parameters
        ----------
        G
            Graph object to use.
        pipeline_name
            Name of the pipeline.
        metrics
            Metrics to optimize for.
        model_name
            Name of the trained model.
        target_property
            Target node property to predict.
        relationship_types
            Relationship type filter.
        target_node_labels
            Node label filter.
        store_model_to_disk
            Whether to persist the trained model to disk.
        random_seed
            Seed for random number generation.
        username
            Optional impersonated user.
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
            Estimated memory footprint for training.
        """
        pass
