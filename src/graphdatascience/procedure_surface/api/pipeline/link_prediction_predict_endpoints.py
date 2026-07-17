from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class LinkPredictionPipelinePredictEndpoints(ABC):
    @abstractmethod
    def estimate(
        self,
        G: Graph,
        model_name: str,
        *,
        source_node_label: str | None = None,
        target_node_label: str | None = None,
        top_n: int | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory required to run link prediction.

        Parameters
        ----------
        G
            Graph object to use
        model_name
            Name of the model.
        source_node_label
            Node label to consider as source nodes for candidate links.
        target_node_label
            Node label to consider as target nodes for candidate links.
        top_n
            Keep only the `top_n` predicted links with the highest probability.
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
            The estimated memory footprint for prediction.
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: Graph,
        model_name: str,
        *,
        relationship_types: list[str] | None = None,
        sample_rate: float = 1.0,
        source_node_label: str | None = None,
        target_node_label: str | None = None,
        threshold: float | None = None,
        top_k: int | None = None,
        top_n: int | None = None,
        initial_sampler: str | None = None,
        delta_threshold: float | None = None,
        max_iterations: int | None = None,
        random_joins: int | None = None,
        random_seed: int | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Run link prediction in stream mode.

        Parameters
        ----------
        G
            Graph object to use
        model_name
            Name of the model.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sample_rate
            Sample rate used to limit the number of candidate node pairs considered.
        source_node_label
            Node label to consider as source nodes for candidate links.
        target_node_label
            Node label to consider as target nodes for candidate links.
        threshold
            Only predicted links with a probability of at least this value are returned.
        top_k
            Number of predicted links to retain per node.
        top_n
            Keep only the `top_n` predicted links with the highest probability.
        initial_sampler
            The sampler used to generate the initial set of candidate node pairs (e.g. `"UNIFORM"` or `"RANDOMWALK"`).
        delta_threshold
            Convergence threshold used by the approximate strategy.
        max_iterations
            Maximum number of iterations for the approximate strategy.
        random_joins
            Number of random join attempts per node used by the approximate strategy.
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
        DataFrame
            The predicted links as a DataFrame.
        """
        pass

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        model_name: str,
        mutate_relationship_type: str,
        *,
        mutate_property: str = "probability",
        relationship_types: list[str] | None = None,
        sample_rate: float = 1.0,
        source_node_label: str | None = None,
        target_node_label: str | None = None,
        threshold: float | None = None,
        top_k: int | None = None,
        top_n: int | None = None,
        initial_sampler: str | None = None,
        delta_threshold: float | None = None,
        max_iterations: int | None = None,
        random_joins: int | None = None,
        random_seed: int | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> LinkPredictionPipelinePredictMutateResult:
        """
        Run link prediction in mutate mode, writing the predicted links back to the in-memory graph.

        Parameters
        ----------
        G
            Graph object to use
        model_name
            Name of the model.
        mutate_relationship_type
            Name of the relationship type to store the results in.
        mutate_property
            Name of the relationship property to store the predicted probability.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        sample_rate
            Sample rate used to limit the number of candidate node pairs considered.
        source_node_label
            Node label to consider as source nodes for candidate links.
        target_node_label
            Node label to consider as target nodes for candidate links.
        threshold
            Only predicted links with a probability of at least this value are written.
        top_k
            Number of predicted links to retain per node.
        top_n
            Keep only the `top_n` predicted links with the highest probability.
        initial_sampler
            The sampler used to generate the initial set of candidate node pairs (e.g. `"UNIFORM"` or `"RANDOMWALK"`).
        delta_threshold
            Convergence threshold used by the approximate strategy.
        max_iterations
            Maximum number of iterations for the approximate strategy.
        random_joins
            Number of random join attempts per node used by the approximate strategy.
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
        LinkPredictionPipelinePredictMutateResult
            Metrics and statistics about the mutate operation.
        """
        pass


class LinkPredictionPipelinePredictMutateResult(BaseResult):
    compute_millis: int | None = None
    configuration: dict[str, Any] | None = None
    mutate_millis: int | None = None
    post_processing_millis: int | None = None
    pre_processing_millis: int | None = None
    probability_distribution: dict[str, Any] | None = None
    relationships_written: int | None = None
    sampling_stats: dict[str, Any] | None = None
