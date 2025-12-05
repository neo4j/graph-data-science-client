from __future__ import annotations

from abc import ABC, abstractmethod

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2


class NodeLabelEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        node_label: str,
        *,
        node_filter: str,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        write_concurrency: int | None = None,
        job_id: str | None = None,
    ) -> NodeLabelMutateResult:
        """
        Attaches the specified node label to the filtered nodes in the graph.

        Parameters
        ----------
        G
           Graph object to use
        node_label : str
            The node label to write back.
        node_filter : str
            A Cypher predicate for filtering nodes in the input graph.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        write_concurrency
            Number of concurrent threads to use for writing.
        job_id
            Identifier for the computation.
        Returns
        -------
        NodeLabelMutateResult
            Execution metrics and statistics
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        node_label: str,
        *,
        node_filter: str,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        write_concurrency: int | None = None,
        job_id: str | None = None,
    ) -> NodeLabelWriteResult:
        """
        Writes the specified node label to the filtered nodes in the database.

        Parameters
        ----------
        G
           Graph object to use
        node_label : str
            The node label to write back.
        node_filter : str
            A Cypher predicate for filtering nodes in the input graph.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        write_concurrency
            Number of concurrent threads to use for writing.
        job_id
            Identifier for the computation.
        Returns
        -------
        NodeLabelWriteResult
            Execution metrics and statistics
        """
        pass


class NodeLabelPersistenceResult(ABC, BaseResult):
    graph_name: str
    node_label: str
    node_count: int
    node_labels_written: int
    configuration: dict[str, object]


class NodeLabelMutateResult(NodeLabelPersistenceResult):
    mutate_millis: int


class NodeLabelWriteResult(NodeLabelPersistenceResult):
    write_millis: int
