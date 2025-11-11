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
        G : GraphV2
            The graph to run the algorithm on
        node_label : str
            The node label to write back.
        node_filter : str
            A Cypher predicate for filtering nodes in the input graph.
        sudo : bool
            Disable the memory guard.
        log_progress : bool, default=True
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None, default=None
            The number of concurrent threads
        write_concurrency : int | None, default=None
            The number of concurrent threads used for the mutation
        job_id : str | None, default=None
            An identifier for the job
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
        G : GraphV2
            The graph to run the algorithm on
        node_label : str
            The node label to write back.
        node_filter : str
            A Cypher predicate for filtering nodes in the input graph.
        sudo : bool
            Disable the memory guard.
        log_progress : bool, default=True
            Whether to log progress
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None
            The number of concurrent threads
        write_concurrency : int | None
            The number of concurrent threads used for the mutation
        job_id : str | None
            An identifier for the job
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
