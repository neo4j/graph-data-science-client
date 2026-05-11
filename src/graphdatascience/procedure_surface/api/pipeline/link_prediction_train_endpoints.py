from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.model.link_prediction_model import LinkPredictionModelV2
from graphdatascience.procedure_surface.api.pipeline.link_prediction_pipeline_results import (
    LinkPredictionPipelineTrainResult,
)


class LinkPredictionPipelineTrainEndpoints(ABC):
    @abstractmethod
    def __call__(
        self,
        G: GraphV2,
        pipeline_name: str,
        *,
        model_name: str,
        metrics: list[str] | None = None,
        source_node_label: str | None = None,
        target_node_label: str | None = None,
        target_relationship_type: str | None = None,
        store_model_to_disk: bool = False,
        random_seed: Any | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> tuple[LinkPredictionModelV2, LinkPredictionPipelineTrainResult]:
        """
        Train a link prediction model from the specified pipeline.

        Parameters
        ----------
        G
            Graph object to use
        pipeline_name
            Name of the pipeline.
        model_name
            Name of the trained model.
        metrics
            Metrics to optimize for.
        source_node_label
            Source node label filter.
        target_node_label
            Target node label filter.
        target_relationship_type
            Target relationship type filter.
        store_model_to_disk
            Whether to persist the trained model to disk.
        random_seed
            Seed for random number generation.
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
        tuple[LinkPredictionModelV2, LinkPredictionPipelineTrainResult]
            Trained model and training result.
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2,
        pipeline_name: str,
        *,
        model_name: str,
        metrics: list[str] | None = None,
        source_node_label: str | None = None,
        target_node_label: str | None = None,
        target_relationship_type: str | None = None,
        store_model_to_disk: bool = False,
        random_seed: Any | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> EstimationResult:
        """
        Estimate memory required to train a link prediction model.

        Parameters
        ----------
        G
            Graph object to use
        pipeline_name
            Name of the pipeline.
        model_name
            Name of the trained model.
        metrics
            Metrics to optimize for.
        source_node_label
            Source node label filter.
        target_node_label
            Target node label filter.
        target_relationship_type
            Target relationship type filter.
        store_model_to_disk
            Whether to persist the trained model to disk.
        random_seed
            Seed for random number generation.
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
            Estimated memory footprint for training.
        """
        pass
