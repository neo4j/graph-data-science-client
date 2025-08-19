from __future__ import annotations

from typing import Optional

from pandas import DataFrame

from graphdatascience.model.v2.model_api import ModelApi
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.graphsage_predict_endpoints import (
    GraphSageMutateResult,
    GraphSagePredictEndpoints,
    GraphSageWriteResult,
)

from ....graph.graph_object import Graph
from ....graph.graph_type_check import graph_type_check
from ....model.v2.model import Model


class GraphSageModelV2(Model):
    """
    Represents a GraphSAGE model in the model catalog.
    Construct this using :func:`gds.graphSage.train()`.
    """

    def __init__(self, name: str, model_api: ModelApi, predict_endpoints: GraphSagePredictEndpoints) -> None:
        super().__init__(name, model_api)
        self._predict_endpoints = predict_endpoints

    @graph_type_check
    def predict_write(
        self,
        G: Graph,
        write_property: str,
        relationship_types: Optional[list[str]] = None,
        node_labels: Optional[list[str]] = None,
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        write_concurrency: Optional[int] = None,
        write_to_result_store: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        sudo: Optional[bool] = None,
        job_id: Optional[str] = None,
    ) -> GraphSageWriteResult:
        """
        Generate embeddings for the given graph and write the results to the database.

        Args:
            G: The graph to generate embeddings for.
            write_property: The property to write the embeddings to.
            relationship_types: The relationship types to consider.
            node_labels: The node labels to consider.
            batch_size: The batch size for prediction.
            concurrency: The concurrency for computation.
            write_concurrency: The concurrency for writing.
            write_to_result_store: Whether to write to the result store.
            log_progress: Whether to log progress.
            username: The username for the operation.
            sudo: Whether to use sudo privileges.
            job_id: The job ID for the operation.

        Returns:
            The result of the write operation.

        """
        return self._predict_endpoints.write(
            G,
            modelName=self.name(),
            writeProperty=write_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            batchSize=batch_size,
            concurrency=concurrency,
            writeConcurrency=write_concurrency,
            writeToResultStore=write_to_result_store,
            logProgress=log_progress,
            username=username,
            sudo=sudo,
            jobId=job_id,
        )

    def predict_stream(
        self,
        G: Graph,
        relationship_types: Optional[list[str]] = None,
        node_labels: Optional[list[str]] = None,
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        sudo: Optional[bool] = None,
        job_id: Optional[str] = None,
    ) -> DataFrame:
        """
        Generate embeddings for the given graph and stream the results.

        Args:
            G: The graph to generate embeddings for.
            relationship_types: The relationship types to consider.
            node_labels: The node labels to consider.
            batch_size: The batch size for prediction.
            concurrency: The concurrency for computation.
            log_progress: Whether to log progress.
            username: The username for the operation.
            sudo: Whether to use sudo privileges.
            job_id: The job ID for the operation.

        Returns:
            The streaming results as a DataFrame.

        """
        return self._predict_endpoints.stream(
            G,
            modelName=self.name(),
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            batchSize=batch_size,
            concurrency=concurrency,
            logProgress=log_progress,
            username=username,
            sudo=sudo,
            jobId=job_id,
        )

    def predict_mutate(
        self,
        G: Graph,
        mutate_property: str,
        relationship_types: Optional[list[str]] = None,
        node_labels: Optional[list[str]] = None,
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        sudo: Optional[bool] = None,
        job_id: Optional[str] = None,
    ) -> GraphSageMutateResult:
        """
        Generate embeddings for the given graph and mutate the graph with the results.

        Args:
            G: The graph to generate embeddings for.
            mutate_property: The property to mutate with the embeddings.
            relationship_types: The relationship types to consider.
            node_labels: The node labels to consider.
            batch_size: The batch size for prediction.
            concurrency: The concurrency for computation.
            log_progress: Whether to log progress.
            username: The username for the operation.
            sudo: Whether to use sudo privileges.
            job_id: The job ID for the operation.

        Returns:
            The result of the mutate operation.

        """
        return self._predict_endpoints.mutate(
            G,
            modelName=self.name(),
            mutateProperty=mutate_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            batchSize=batch_size,
            concurrency=concurrency,
            logProgress=log_progress,
            username=username,
            sudo=sudo,
            jobId=job_id,
        )

    @graph_type_check
    def predict_estimate(
        self,
        G: Graph,
        relationship_types: Optional[list[str]] = None,
        node_labels: Optional[list[str]] = None,
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        sudo: Optional[bool] = None,
        job_id: Optional[str] = None,
    ) -> EstimationResult:
        """
        Estimate the memory needed to generate embeddings for the given graph and write the results to the database.

        Args:
            G: The graph to generate embeddings for.
            relationship_types: The relationship types to consider.
            node_labels: The node labels to consider.
            batch_size: The batch size for prediction.
            concurrency: The concurrency for computation.
            log_progress: Whether to log progress.
            username: The username for the operation.
            sudo: Whether to use sudo privileges.
            job_id: The job ID for the operation.

        Returns:
            The memory needed to generate embeddings for the given graph and write the results to the database.

        """
        return self._predict_endpoints.estimate(
            G,
            modelName=self.name(),
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            batchSize=batch_size,
            concurrency=concurrency,
            logProgress=log_progress,
            username=username,
            sudo=sudo,
            jobId=job_id,
        )
