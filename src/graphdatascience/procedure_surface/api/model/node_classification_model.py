from __future__ import annotations

from pandas import DataFrame

from graphdatascience.graph.graph_api import Graph
from graphdatascience.model.v2.model import Model
from graphdatascience.model.v2.model_api import ModelApi
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pipeline.node_classification_predict_endpoints import (
    NodeClassificationPipelinePredictEndpoints,
    NodeClassificationPipelinePredictMutateResult,
    NodeClassificationPipelinePredictWriteResult,
)


class NodeClassificationModelV2(Model):
    """
    Represents a node classification model in the model catalog.

    Construct this using: func:`gds.v2.pipeline.node_classification.train()`.
    """

    def __init__(
        self, name: str, model_api: ModelApi, predict_endpoints: NodeClassificationPipelinePredictEndpoints
    ) -> None:
        super().__init__(name, model_api)
        self._predict_endpoints = predict_endpoints

    def predict_stream(
        self,
        G: Graph,
        *,
        relationship_types: list[str] | None = None,
        target_node_labels: list[str] | None = None,
        include_predicted_probabilities: bool = False,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Run prediction and stream the results.

        Parameters
        ----------
        G
            Graph object to use
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        target_node_labels
            Optional node label filter.
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
            The prediction results as a DataFrame.
        """
        return self._predict_endpoints.stream(
            G,
            model_name=self.name(),
            relationship_types=relationship_types,
            target_node_labels=target_node_labels,
            include_predicted_probabilities=include_predicted_probabilities,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )

    def predict_estimate(
        self,
        G: Graph,
        *,
        relationship_types: list[str] | None = None,
        target_node_labels: list[str] | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory required to run prediction.

        Parameters
        ----------
        G
            Graph object to use
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        target_node_labels
            Optional node label filter.
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
        return self._predict_endpoints.estimate(
            G,
            model_name=self.name(),
            relationship_types=relationship_types,
            target_node_labels=target_node_labels,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )

    def predict_mutate(
        self,
        G: Graph,
        mutate_property: str,
        *,
        relationship_types: list[str] | None = None,
        target_node_labels: list[str] | None = None,
        predicted_probability_property: str | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> NodeClassificationPipelinePredictMutateResult:
        """
        Run prediction and write the results back to the in-memory graph.

        Parameters
        ----------
        G
            Graph object to use
        mutate_property
            Name of the node property to store the results in.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        target_node_labels
            Optional node label filter.
        predicted_probability_property
            Optional node property to store the predicted probability distribution in.
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
        NodeClassificationPipelinePredictMutateResult
            The mutate result summary.
        """
        return self._predict_endpoints.mutate(
            G,
            model_name=self.name(),
            mutate_property=mutate_property,
            relationship_types=relationship_types,
            target_node_labels=target_node_labels,
            predicted_probability_property=predicted_probability_property,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            job_id=job_id,
        )

    def predict_write(
        self,
        G: Graph,
        write_property: str,
        *,
        relationship_types: list[str] | None = None,
        target_node_labels: list[str] | None = None,
        predicted_probability_property: str | None = None,
        username: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        concurrency: int | None = None,
        write_concurrency: int | None = None,
        job_id: str | None = None,
    ) -> NodeClassificationPipelinePredictWriteResult:
        """
        Run prediction and write the results back to Neo4j.

        Parameters
        ----------
        G
            Graph object to use
        write_property
            Name of the node property to store the results in.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        target_node_labels
            Optional node label filter.
        predicted_probability_property
            Optional node property to store the predicted probability distribution in.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        log_progress
            Display progress logging.
        sudo
            Disable the memory guard.
        concurrency
            Number of concurrent threads to use.
        write_concurrency
            Number of concurrent threads to use for writing.
        job_id
            Identifier for the computation.

        Returns
        -------
        NodeClassificationPipelinePredictWriteResult
            The write result summary.
        """
        return self._predict_endpoints.write(
            G,
            model_name=self.name(),
            write_property=write_property,
            relationship_types=relationship_types,
            target_node_labels=target_node_labels,
            predicted_probability_property=predicted_probability_property,
            username=username,
            log_progress=log_progress,
            sudo=sudo,
            concurrency=concurrency,
            write_concurrency=write_concurrency,
            job_id=job_id,
        )
