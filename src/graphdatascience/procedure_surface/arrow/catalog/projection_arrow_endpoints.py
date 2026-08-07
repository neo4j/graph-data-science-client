from __future__ import annotations

import time
import typing
import uuid
from types import TracebackType
from typing import Any

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.projection_job_handle import ProjectionJobHandle
from graphdatascience.procedure_surface.arrow.catalog.graph_backend_arrow import get_graph
from graphdatascience.procedure_surface.arrow.catalog.graph_ops_arrow import GraphOpsArrow
from graphdatascience.query_runner import QueryRunner
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.session.dbms.protocol_resolver import ProtocolVersionResolver
from graphdatascience.session.remote_ops.project_protocols import ProjectProtocol
from graphdatascience.session.remote_ops.projection_runner import ProjectionRunner


class ProjectArrowEndpoints:
    """Endpoints for projecting graphs via the Arrow pipeline."""

    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        query_runner: QueryRunner | None = None,
        show_progress: bool = False,
    ):
        self._arrow_client = arrow_client
        self._query_runner = query_runner
        self._graph_backend = GraphOpsArrow(arrow_client)
        self._show_progress = show_progress
        if query_runner is not None:
            protocol_version = ProtocolVersionResolver(query_runner).resolve()
            self._project_protocol = ProjectProtocol.select(
                protocol_version, arrow_client, query_runner, TerminationFlag.create()
            )

    def cypher(
        self,
        graph_name: str,
        query: str,
        *,
        query_parameters: dict[str, Any] | None = None,
        job_id: str | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: typing.List[str] | None = None,
        inverse_indexed_relationship_types: typing.List[str] | None = None,
        batch_size: int | None = None,
        logging: bool = True,
    ) -> GraphWithProjectResult:
        """
        Projects a graph from the Neo4j database into the GDS graph catalog using Cypher projection.

        Parameters
        ----------
        graph_name
            Name of the graph to be created in the catalog.
        query
            Cypher query to select nodes and relationships for the graph projection.
            Must contain `gds.graph.project.remote`. Example: `MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)`
        query_parameters
            Parameters that will be passed to the Cypher query.
        job_id
            Identifier for the computation.
        concurrency
            Number of concurrent threads to use.
        undirected_relationship_types : list[str]
            List of relationship types to treat as undirected.
        inverse_indexed_relationship_types : list[str]
            List of relationship types to index in both directions.
        batch_size : int | None
            Number of rows to process in each batch when projecting the graph.
        logging : bool
            Whether to log progress during graph projection.
        Returns
        -------
        GraphWithProjectResult:
            A result object containing information about the projected graph.
        """
        if self._query_runner is None:
            raise ValueError("Remote projection is only supported for attached Sessions.")

        job_id = job_id or str(uuid.uuid4())
        logging = self._show_progress and logging

        ProjectionRunner(self._project_protocol, self._arrow_client, TerminationFlag.create()).run_cypher_projection(
            graph_name,
            query,
            job_id,
            query_parameters,
            concurrency,
            undirected_relationship_types,
            inverse_indexed_relationship_types,
            batch_size,
            logging,
        )

        job_result = ProjectionResult(**JobClient.get_summary(self._arrow_client, job_id))

        return GraphWithProjectResult(get_graph(graph_name, self._arrow_client), job_result)

    def cypher_async(
        self,
        graph_name: str,
        query: str,
        *,
        query_parameters: dict[str, Any] | None = None,
        job_id: str | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: typing.List[str] | None = None,
        inverse_indexed_relationship_types: typing.List[str] | None = None,
        batch_size: int | None = None,
    ) -> ProjectionJobHandle:
        """Kick off a cypher graph projection and return a :class:`~graphdatascience.procedure_surface.api.projection_job_handle.ProjectionJobHandle`.

        Unlike :meth:`cypher`, this method does not block on completion. Use the
        returned handle to query status or retrieve the projected graph and result.
        """
        if self._query_runner is None:
            raise ValueError("Remote projection is only supported for attached Sessions.")

        job_id = job_id or str(uuid.uuid4())

        actual_job_id, projection_query_runner = self._project_protocol.start_cypher_projection(
            graph_name,
            query,
            job_id,
            query_parameters,
            concurrency,
            undirected_relationship_types,
            inverse_indexed_relationship_types,
            batch_size,
        )

        # get the status at least once to make sure the job is actually running
        self._project_protocol.get_status(actual_job_id, projection_query_runner)
        projection_query_runner.close()

        return ProjectionJobHandle(self._arrow_client, graph_name, actual_job_id, TerminationFlag.create())

    def native(
        self,
        graph_name: str,
        node_label_filter: typing.List[str],
        relationship_type_filter: typing.List[str],
        *,
        node_properties: typing.List[str] | None = None,
        relationship_properties: typing.List[str] | None = None,
        job_id: str | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: typing.List[str] | None = None,
        inverse_indexed_relationship_types: typing.List[str] | None = None,
        batch_size: int | None = None,
        logging: bool = True,
    ) -> GraphWithProjectResult:
        """
        Projects a graph from the Neo4j database into the GDS graph catalog.

        Parameters
        ----------
        graph_name : str
            Name of the graph to be created in the catalog.
        node_label_filter : list[str]
            List of node labels to include in the graph projection.
        relationship_type_filter : list[str]
            List of relationship types to include in the graph projection.
        node_properties : list[str]
            List of node properties to include in the graph projection.
        relationship_properties : list[str]
            List of relationship properties to include in the graph projection.
        job_id
            Identifier for the computation.
        concurrency
            Number of concurrent threads to use.
        undirected_relationship_types : list[str]
            List of relationship types to treat as undirected.
        inverse_indexed_relationship_types : list[str]
            List of relationship types to index in both directions.
        batch_size : int | None
            Number of rows to process in each batch when projecting the graph.
        logging : bool
            Whether to log progress during graph projection.
        Returns
        -------
        ProjectionResult:
            A result object containing information about the projected graph.
        """

        start = time.time()

        if self._query_runner is None:
            raise ValueError("Remote projection is only supported for attached Sessions.")

        job_id = job_id or str(uuid.uuid4())
        logging = self._show_progress and logging

        ProjectionRunner(self._project_protocol, self._arrow_client, TerminationFlag.create()).run_store_projection(
            graph_name,
            node_label_filter,
            relationship_type_filter,
            node_properties,
            relationship_properties,
            job_id,
            concurrency,
            undirected_relationship_types,
            inverse_indexed_relationship_types,
            batch_size,
            logging,
        )

        project_millis = int((time.time() - start) * 1000)

        summary = JobClient.get_summary(self._arrow_client, job_id)
        job_result = StoreProjectionResult(projectMillis=project_millis, **summary)

        return GraphWithProjectResult(get_graph(graph_name, self._arrow_client), job_result)

    def native_async(
        self,
        graph_name: str,
        node_label_filter: typing.List[str],
        relationship_type_filter: typing.List[str],
        *,
        node_properties: typing.List[str] | None = None,
        relationship_properties: typing.List[str] | None = None,
        job_id: str | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: typing.List[str] | None = None,
        inverse_indexed_relationship_types: typing.List[str] | None = None,
        batch_size: int | None = None,
    ) -> ProjectionJobHandle:
        """Kick off a native graph projection and return a :class:`~graphdatascience.procedure_surface.api.projection_job_handle.ProjectionJobHandle`.

        Unlike :meth:`native`, this method does not block on completion.
        The returned handle can be used to await completion and retrieve the
        projected graph and result.
        """
        if self._query_runner is None:
            raise ValueError("Remote projection is only supported for attached Sessions.")

        job_id = job_id or str(uuid.uuid4())

        actual_job_id, projection_query_runner = self._project_protocol.start_store_projection(
            graph_name,
            node_label_filter,
            relationship_type_filter,
            node_properties,
            relationship_properties,
            job_id,
            concurrency,
            undirected_relationship_types,
            inverse_indexed_relationship_types,
            batch_size,
        )

        self._project_protocol.get_status(actual_job_id, projection_query_runner)
        projection_query_runner.close()

        return ProjectionJobHandle(self._arrow_client, graph_name, actual_job_id, TerminationFlag.create())


class ProjectionResult(BaseResult):
    """Result object for graph projection jobs."""

    graph_name: str
    node_count: int
    relationship_count: int
    project_millis: int
    configuration: dict[str, Any]
    query: str


class StoreProjectionResult(BaseResult):
    """Result object for native graph projection jobs."""

    graph_name: str
    node_count: int
    relationship_count: int
    project_millis: int


class GraphWithProjectResult(typing.NamedTuple):
    """Result object for graph projection jobs, containing the projected graph and the projection result.
    Can be used as a context manager to ensure the projected graph is dropped after use."""

    graph: Graph
    result: ProjectionResult | StoreProjectionResult

    def __enter__(self) -> Graph:
        return self.graph

    def __exit__(
        self,
        exception_type: typing.Type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.graph.drop()
