from __future__ import annotations

from pandas import DataFrame

from graphdatascience.model.v2.model_api import ModelApi
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.node_embedding.graphsage_predict_endpoints import (
    GraphSageMutateResult,
    GraphSagePredictEndpoints,
    GraphSageWriteResult,
)

from ....model.v2.model import Model


class GraphSageModelV2(Model):
    """
    Represents a GraphSAGE model in the model catalog.
    Construct this using :func:`gds.v2.graphSage.train()`.
    """

    def __init__(self, name: str, model_api: ModelApi, predict_endpoints: GraphSagePredictEndpoints) -> None:
        super().__init__(name, model_api)
        self._predict_endpoints = predict_endpoints

    def predict_write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        batch_size: int | None = None,
        concurrency: int | None = None,
        write_concurrency: int | None = None,
        log_progress: bool = True,
        username: str | None = None,
        sudo: bool | None = None,
        job_id: str | None = None,
    ) -> GraphSageWriteResult:
        """
        Generate embeddings for the given graph and write the results to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to generate embeddings for.
        write_property : str
            The property to write the embeddings to.
        relationship_types : list[str] | None, default=None
            The relationship types to consider.
        node_labels : list[str] | None, default=None
            The node labels to consider.
        batch_size : int | None, default=None
            The batch size for prediction.
        concurrency : int | None, default=None
            The concurrency for computation.
        write_concurrency : int | None, default=None
            The concurrency for writing.
        log_progress : bool | None, default=None
            Whether to log progress.
        username : str | None, default=None
            The username for the operation.
        sudo : bool | None, default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        job_id : str | None, default=None
            The job ID for the operation.

        Returns
        -------
        GraphSageWriteResult
            The result of the write operation.

        """
        return self._predict_endpoints.write(
            G,
            model_name=self.name(),
            write_property=write_property,
            relationship_types=relationship_types,
            node_labels=node_labels,
            batch_size=batch_size,
            concurrency=concurrency,
            write_concurrency=write_concurrency,
            log_progress=log_progress,
            username=username,
            sudo=sudo,
            job_id=job_id,
        )

    def predict_stream(
        self,
        G: GraphV2,
        *,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        batch_size: int | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        username: str | None = None,
        sudo: bool | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Generate embeddings for the given graph and stream the results.

        Parameters
        ----------
        G : GraphV2
            The graph to generate embeddings for.
        relationship_types : list[str] | None, default=None
            The relationship types to consider.
        node_labels : list[str] | None, default=None
            The node labels to consider.
        batch_size : int | None, default=None
            The batch size for prediction.
        concurrency : int | None, default=None
            The concurrency for computation.
        log_progress : bool | None, default=None
            Whether to log progress.
        username : str | None, default=None
            The username for the operation.
        sudo : bool | None, default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        job_id : str | None, default=None
            The job ID for the operation.

        Returns
        -------
        DataFrame
            The streaming results as a DataFrame.

        """
        return self._predict_endpoints.stream(
            G,
            model_name=self.name(),
            relationship_types=relationship_types,
            node_labels=node_labels,
            batch_size=batch_size,
            concurrency=concurrency,
            log_progress=log_progress,
            username=username,
            sudo=sudo,
            job_id=job_id,
        )

    def predict_mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        batch_size: int | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        username: str | None = None,
        sudo: bool | None = None,
        job_id: str | None = None,
    ) -> GraphSageMutateResult:
        """
        Generate embeddings for the given graph and mutate the graph with the results.

        Parameters
        ----------
        G : GraphV2
            The graph to generate embeddings for.
        mutate_property : str
            The property to mutate with the embeddings.
        relationship_types : list[str] | None, default=None
            The relationship types to consider.
        node_labels : list[str] | None, default=None
            The node labels to consider.
        batch_size : int | None, default=None
            The batch size for prediction.
        concurrency : int | None, default=None
            The concurrency for computation.
        log_progress : bool | None, default=None
            Whether to log progress.
        username : str | None, default=None
            The username for the operation.
        sudo : bool | None, default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        job_id : str | None, default=None
            The job ID for the operation.

        Returns
        -------
        GraphSageMutateResult
            The result of the mutate operation.

        """
        return self._predict_endpoints.mutate(
            G,
            model_name=self.name(),
            mutate_property=mutate_property,
            relationship_types=relationship_types,
            node_labels=node_labels,
            batch_size=batch_size,
            concurrency=concurrency,
            log_progress=log_progress,
            username=username,
            sudo=sudo,
            job_id=job_id,
        )

    def predict_estimate(
        self,
        G: GraphV2,
        *,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        batch_size: int | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        username: str | None = None,
        sudo: bool | None = None,
        job_id: str | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory needed to generate embeddings for the given graph and write the results to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to generate embeddings for.
        relationship_types : list[str] | None, default=None
            The relationship types to consider.
        node_labels : list[str] | None, default=None
            The node labels to consider.
        batch_size : int | None, default=None
            The batch size for prediction.
        concurrency : int | None, default=None
            The concurrency for computation.
        log_progress : bool | None, default=None
            Whether to log progress.
        username : str | None, default=None
            The username for the operation.
        sudo : bool | None, default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        job_id : str | None, default=None
            The job ID for the operation.

        Returns
        -------
        EstimationResult
            The memory needed to generate embeddings for the given graph and write the results to the database.

        """
        return self._predict_endpoints.estimate(
            G,
            model_name=self.name(),
            relationship_types=relationship_types,
            node_labels=node_labels,
            batch_size=batch_size,
            concurrency=concurrency,
            log_progress=log_progress,
            username=username,
            sudo=sudo,
            job_id=job_id,
        )
