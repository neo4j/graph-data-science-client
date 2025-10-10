from __future__ import annotations

import builtins
from types import TracebackType
from typing import Any, NamedTuple, Type
from uuid import uuid4

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.catalog_endpoints import (
    CatalogEndpoints,
    GraphFilterResult,
    GraphGenerationStats,
    GraphWithFilterResult,
    GraphWithGenerationStats,
    RelationshipPropertySpec,
)
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.graph_info import GraphInfo, GraphInfoWithDegrees
from graphdatascience.procedure_surface.api.catalog.graph_sampling_endpoints import GraphSamplingEndpoints
from graphdatascience.procedure_surface.arrow.catalog.graph_backend_arrow import get_graph
from graphdatascience.procedure_surface.arrow.catalog.graph_ops_arrow import GraphOpsArrow
from graphdatascience.procedure_surface.arrow.catalog.graph_sampling_arrow_endpoints import GraphSamplingArrowEndpoints
from graphdatascience.procedure_surface.arrow.catalog.node_label_arrow_endpoints import NodeLabelArrowEndpoints
from graphdatascience.procedure_surface.arrow.catalog.node_properties_arrow_endpoints import (
    NodePropertiesArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.catalog.relationship_arrow_endpoints import RelationshipArrowEndpoints
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.protocol.project_protocols import ProjectProtocol
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.session.dbms.protocol_resolver import ProtocolVersionResolver


class CatalogArrowEndpoints(CatalogEndpoints):
    GDS_REMOTE_PROJECTION_PROC_NAME = "gds.arrow.project"

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
            self._project_protocol = ProjectProtocol.select(protocol_version)

    def list(self, G: GraphV2 | str | None = None) -> list[GraphInfoWithDegrees]:
        graph_name: str | None = None
        if isinstance(G, GraphV2):
            graph_name = G.name()
        elif isinstance(G, str):
            graph_name = G

        return self._graph_backend.list(graph_name)

    def project(
        self,
        graph_name: str,
        query: str,
        *,
        job_id: str | None = None,
        concurrency: int = 4,
        undirected_relationship_types: builtins.list[str] | None = None,
        inverse_indexed_relationship_types: builtins.list[str] | None = None,
        batch_size: int | None = None,
        logging: bool = True,
    ) -> GraphWithProjectResult:
        """
        Projects a graph from the Neo4j database into the GDS graph catalog.

        Parameters
        ----------
        graph_name : str
            Name of the graph to be created in the catalog.
        query : str
            Cypher query to select nodes and relationships for the graph projection.
            Must contain `gds.graph.project.remote`. Example: `MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)`
        job_id : str | None, default=None
            Unique identifier for the projection job.
        concurrency : int, default=4
            Number of concurrent threads/processes to use during graph projection.
        undirected_relationship_types : list[str] | None, default=None
            List of relationship types to treat as undirected.
        inverse_indexed_relationship_types : list[str] | None, default=None
            List of relationship types to index in both directions.
        batch_size : int | None, default=None
            Number of rows to process in each batch when projecting the graph.
        logging : bool, default=True
            Whether to log progress during graph projection.
        Returns
        -------
        ProjectionResult:
            A result object containing information about the projected graph.
        """
        if self._query_runner is None:
            raise ValueError("Remote projection is only supported for attached Sessions.")

        if inverse_indexed_relationship_types is None:
            inverse_indexed_relationship_types = []
        if undirected_relationship_types is None:
            undirected_relationship_types = []

        arrow_config = self._arrow_config()
        if batch_size is not None:
            arrow_config["batchSize"] = batch_size

        job_id = job_id if job_id is not None else str(uuid4())

        params = {
            "concurrency": concurrency,
            "undirected_relationship_types": undirected_relationship_types,
            "inverse_indexed_relationship_types": inverse_indexed_relationship_types,
        }

        project_params = self._project_protocol.project_params(graph_name, query, job_id, params, arrow_config)

        self._project_protocol.run_projection(
            self._query_runner,
            CatalogArrowEndpoints.GDS_REMOTE_PROJECTION_PROC_NAME,
            project_params,
            TerminationFlag.create(),
            None,
            None,
            self._show_progress and logging,
        )

        job_result = ProjectionResult(**JobClient.get_summary(self._arrow_client, job_id))

        return GraphWithProjectResult(get_graph(graph_name, self._arrow_client), job_result)

    def drop(self, G: GraphV2 | str, fail_if_missing: bool = True) -> GraphInfo | None:
        graph_name = G.name() if isinstance(G, GraphV2) else G

        return self._graph_backend.drop(graph_name, fail_if_missing)

    def filter(
        self,
        G: GraphV2,
        graph_name: str,
        node_filter: str,
        relationship_filter: str,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> GraphWithFilterResult:
        config = ConfigConverter.convert_to_gds_config(
            from_graph_name=G.name(),
            graph_name=graph_name,
            node_filter=node_filter,
            relationship_filter=relationship_filter,
            concurrency=concurrency,
            job_id=job_id,
        )

        job_id = JobClient.run_job_and_wait(
            self._arrow_client, "v2/graph.project.filter", config, show_progress=self._show_progress
        )

        return GraphWithFilterResult(
            get_graph(graph_name, self._arrow_client),
            GraphFilterResult(**JobClient.get_summary(self._arrow_client, job_id)),
        )

    def generate(
        self,
        graph_name: str,
        node_count: int,
        average_degree: float,
        *,
        relationship_distribution: str | None = None,
        relationship_seed: int | None = None,
        relationship_property: RelationshipPropertySpec | None = None,
        orientation: str | None = None,
        allow_self_loops: bool | None = None,
        read_concurrency: int | None = None,
        job_id: str | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
    ) -> GraphWithGenerationStats:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=graph_name,
            node_count=node_count,
            average_degree=average_degree,
            relationship_distribution=relationship_distribution,
            relationship_seed=relationship_seed,
            relationship_property=relationship_property.model_dump(by_alias=True) if relationship_property else None,
            orientation=orientation,
            allow_self_loops=allow_self_loops,
            read_concurrency=read_concurrency,
            job_id=job_id,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
        )

        show_progress = self._show_progress and log_progress
        job_id = JobClient.run_job_and_wait(
            self._arrow_client, "v2/graph.generate", config, show_progress=show_progress
        )

        return GraphWithGenerationStats(
            get_graph(graph_name, self._arrow_client),
            GraphGenerationStats(**JobClient.get_summary(self._arrow_client, job_id)),
        )

    @property
    def sample(self) -> GraphSamplingEndpoints:
        return GraphSamplingArrowEndpoints(self._arrow_client, show_progress=self._show_progress)

    @property
    def node_labels(self) -> NodeLabelArrowEndpoints:
        write_client = RemoteWriteBackClient(self._arrow_client, self._query_runner) if self._query_runner else None

        return NodeLabelArrowEndpoints(self._arrow_client, write_client, show_progress=self._show_progress)

    @property
    def node_properties(self) -> NodePropertiesArrowEndpoints:
        return NodePropertiesArrowEndpoints(self._arrow_client, self._query_runner)

    @property
    def relationships(self) -> RelationshipArrowEndpoints:
        return RelationshipArrowEndpoints(
            self._arrow_client,
            RemoteWriteBackClient(self._arrow_client, self._query_runner) if self._query_runner else None,
            show_progress=self._show_progress,
        )

    def _arrow_config(self) -> dict[str, Any]:
        connection_info = self._arrow_client.advertised_connection_info()

        token = self._arrow_client.request_token()
        if token is None:
            token = "IGNORED"

        return {
            "host": connection_info.host,
            "port": connection_info.port,
            "token": token,
            "encrypted": connection_info.encrypted,
        }


class ProjectionResult(BaseResult):
    graph_name: str
    node_count: int
    relationship_count: int
    project_millis: int
    configuration: dict[str, Any]
    query: str


class GraphWithProjectResult(NamedTuple):
    graph: GraphV2
    result: ProjectionResult

    def __enter__(self) -> GraphV2:
        return self.graph

    def __exit__(
        self,
        exception_type: Type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.graph.drop()
