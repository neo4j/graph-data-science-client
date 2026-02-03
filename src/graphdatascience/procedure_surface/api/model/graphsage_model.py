from __future__ import annotations

from pandas import DataFrame

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.model.v2.model import Model
from graphdatascience.model.v2.model_api import ModelApi
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.node_embedding.graphsage_predict_endpoints import (
    GraphSageMutateResult,
    GraphSagePredictEndpoints,
    GraphSageWriteResult,
)


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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        batch_size: int = 100,
        concurrency: int | None = None,
        write_concurrency: int | None = None,
        log_progress: bool = True,
        username: str | None = None,
        sudo: bool = False,
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
        relationship_types : list[str]
            The relationship types to consider.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        batch_size : int = 100
            The batch size for prediction.
        concurrency
            Number of concurrent threads to use.
        write_concurrency : int | None, default=None
            The concurrency for writing.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        sudo
            Disable the memory guard.
        job_id
            Identifier for the computation.

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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        batch_size: int = 100,
        concurrency: int | None = None,
        log_progress: bool = True,
        username: str | None = None,
        sudo: bool = False,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Generate embeddings for the given graph and stream the results.

        Parameters
        ----------
        G : GraphV2
            The graph to generate embeddings for.
        relationship_types : list[str]
            The relationship types to consider.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        batch_size : int = 100
            The batch size for prediction.
        concurrency
            Number of concurrent threads to use.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        sudo
            Disable the memory guard.
        job_id
            Identifier for the computation.

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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        batch_size: int = 100,
        concurrency: int | None = None,
        log_progress: bool = True,
        username: str | None = None,
        sudo: bool = False,
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
        relationship_types : list[str]
            The relationship types to consider.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        batch_size : int = 100
            The batch size for prediction.
        concurrency
            Number of concurrent threads to use.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        sudo
            Disable the memory guard.
        job_id
            Identifier for the computation.

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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        batch_size: int = 100,
        concurrency: int | None = None,
        log_progress: bool = True,
        username: str | None = None,
        sudo: bool = False,
        job_id: str | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory needed to generate embeddings for the given graph and write the results to the database.

        Parameters
        ----------
        G : GraphV2
            The graph to generate embeddings for.
        relationship_types : list[str]
            The relationship types to consider.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        batch_size : int = 100
            The batch size for prediction.
        concurrency
            Number of concurrent threads to use.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        sudo
            Disable the memory guard.
        job_id
            Identifier for the computation.

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
