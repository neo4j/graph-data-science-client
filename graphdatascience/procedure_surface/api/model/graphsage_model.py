from __future__ import annotations

from typing import Optional

from pandas import DataFrame

from graphdatascience.model.v2.model_api import ModelApi
from graphdatascience.procedure_surface.api.catalog.graph_api import Graph
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.graphsage_predict_endpoints import (
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
        G: Graph,
        write_property: str,
        *,
        relationship_types: Optional[list[str]] = None,
        node_labels: Optional[list[str]] = None,
        batch_size: Optional[int] = None,
        concurrency: Optional[int] = None,
        write_concurrency: Optional[int] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        sudo: Optional[bool] = None,
        job_id: Optional[str] = None,
    ) -> GraphSageWriteResult:
        """
        Generate embeddings for the given graph and write the results to the database.

        Parameters
        ----------
        G : Graph
            The graph to generate embeddings for.
        write_property : str
            The property to write the embeddings to.
        relationship_types : Optional[list[str]], default=None
            The relationship types to consider.
        node_labels : Optional[list[str]], default=None
            The node labels to consider.
        batch_size : Optional[int], default=None
            The batch size for prediction.
        concurrency : Optional[int], default=None
            The concurrency for computation.
        write_concurrency : Optional[int], default=None
            The concurrency for writing.
        log_progress : Optional[bool], default=None
            Whether to log progress.
        username : Optional[str], default=None
            The username for the operation.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        job_id : Optional[str], default=None
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
        G: Graph,
        *,
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

        Parameters
        ----------
        G : Graph
            The graph to generate embeddings for.
        relationship_types : Optional[list[str]], default=None
            The relationship types to consider.
        node_labels : Optional[list[str]], default=None
            The node labels to consider.
        batch_size : Optional[int], default=None
            The batch size for prediction.
        concurrency : Optional[int], default=None
            The concurrency for computation.
        log_progress : Optional[bool], default=None
            Whether to log progress.
        username : Optional[str], default=None
            The username for the operation.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        job_id : Optional[str], default=None
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
        G: Graph,
        mutate_property: str,
        *,
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

        Parameters
        ----------
        G : Graph
            The graph to generate embeddings for.
        mutate_property : str
            The property to mutate with the embeddings.
        relationship_types : Optional[list[str]], default=None
            The relationship types to consider.
        node_labels : Optional[list[str]], default=None
            The node labels to consider.
        batch_size : Optional[int], default=None
            The batch size for prediction.
        concurrency : Optional[int], default=None
            The concurrency for computation.
        log_progress : Optional[bool], default=None
            Whether to log progress.
        username : Optional[str], default=None
            The username for the operation.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        job_id : Optional[str], default=None
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
        G: Graph,
        *,
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

        Parameters
        ----------
        G : Graph
            The graph to generate embeddings for.
        relationship_types : Optional[list[str]], default=None
            The relationship types to consider.
        node_labels : Optional[list[str]], default=None
            The node labels to consider.
        batch_size : Optional[int], default=None
            The batch size for prediction.
        concurrency : Optional[int], default=None
            The concurrency for computation.
        log_progress : Optional[bool], default=None
            Whether to log progress.
        username : Optional[str], default=None
            The username for the operation.
        sudo : Optional[bool], default=None
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        job_id : Optional[str], default=None
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
