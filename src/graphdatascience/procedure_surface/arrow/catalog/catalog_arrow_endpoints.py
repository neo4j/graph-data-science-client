from __future__ import annotations

import typing
from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.graph.graph_info import GraphInfo, GraphInfoWithDegrees
from graphdatascience.graph_construction.arrow_v2_graph_constructor import ArrowV2GraphConstructor
from graphdatascience.procedure_surface.api.catalog import (
    NodeLabelEndpoints,
    NodePropertiesEndpoints,
    RelationshipsEndpoints,
)
from graphdatascience.procedure_surface.api.catalog.catalog_endpoints import (
    CatalogEndpoints,
    GraphFilterResult,
    GraphGenerationStats,
    GraphWithFilterResult,
    GraphWithGenerationStats,
    RelationshipPropertySpec,
)
from graphdatascience.procedure_surface.api.catalog.graph_export_endpoints import GraphExportEndpoints
from graphdatascience.procedure_surface.api.catalog.graph_sampling_endpoints import GraphSamplingEndpoints
from graphdatascience.procedure_surface.api.projection_job_handle import ProjectionJobHandle
from graphdatascience.procedure_surface.arrow.catalog.graph_backend_arrow import get_graph
from graphdatascience.procedure_surface.arrow.catalog.graph_export_arrow_endpoints import (
    GraphExportArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.catalog.graph_ops_arrow import GraphOpsArrow
from graphdatascience.procedure_surface.arrow.catalog.graph_sampling_arrow_endpoints import GraphSamplingArrowEndpoints
from graphdatascience.procedure_surface.arrow.catalog.node_label_arrow_endpoints import NodeLabelArrowEndpoints
from graphdatascience.procedure_surface.arrow.catalog.node_properties_arrow_endpoints import (
    NodePropertiesArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.catalog.projection_arrow_endpoints import ProjectArrowEndpoints
from graphdatascience.procedure_surface.arrow.catalog.relationship_arrow_endpoints import RelationshipArrowEndpoints
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol


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
        self._write_protocol: WriteProtocol | None = None
        if query_runner is not None:
            self._write_protocol = WriteProtocol.select(arrow_client, query_runner)

    @property
    def project(self) -> ProjectArrowEndpoints:
        return ProjectArrowEndpoints(self._arrow_client, self._query_runner, self._show_progress)

    def get(self, graph_name: str) -> Graph:
        if not self.list(graph_name):
            raise ValueError(f"A graph with name '{graph_name}' does not exist in the catalog.")
        return get_graph(graph_name, self._arrow_client)

    def exists(self, graph_name: str) -> bool:
        return len(self.list(graph_name)) > 0

    def construct(
        self,
        graph_name: str,
        nodes: DataFrame | typing.List[DataFrame],
        relationships: DataFrame | typing.List[DataFrame] | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: typing.List[str] | None = None,
        inverse_index_relationship_types: typing.List[str] | None = None,
        batch_size: int = 100000,
    ) -> Graph:
        if isinstance(nodes, DataFrame):
            nodes = [nodes]
        if relationships is not None and isinstance(relationships, DataFrame):
            relationships = [relationships]
        if relationships is None:
            relationships = []

        constructor = ArrowV2GraphConstructor(
            self._arrow_client,
            graph_name,
            concurrency,
            undirected_relationship_types,
            inverse_index_relationship_types,
            batch_size,
            self._show_progress,
        )
        constructor.run(nodes, relationships)
        return get_graph(graph_name, self._arrow_client)

    def drop(self, G: Graph | str, fail_if_missing: bool = True) -> GraphInfo | None:
        """Drop a graph from the graph catalog.

        Parameters
        ----------
        G
            Graph to drop by name or object.
        fail_if_missing
            Whether to fail if the graph is missing.

        Returns
        -------
        GraphInfo | None
            Metadata of the dropped graph, or None if the graph did not exist.
        """
        graph_name = G.name() if isinstance(G, Graph) else G

        return self._graph_backend.drop(graph_name, fail_if_missing)

    def filter(
        self,
        G: Graph,
        graph_name: str,
        node_filter: str,
        relationship_filter: str,
        parameters: dict[str, Any] | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
    ) -> GraphWithFilterResult:
        config = ConfigConverter.convert_to_gds_config(
            from_graph_name=G.name(),
            graph_name=graph_name,
            node_filter=node_filter,
            relationship_filter=relationship_filter,
            parameters=parameters,
            concurrency=concurrency,
            job_id=job_id,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
        )

        job_id = JobClient.run_job_and_wait(
            self._arrow_client, "v2/graph.project.filter", config, show_progress=self._show_progress
        )

        return GraphWithFilterResult(
            get_graph(graph_name, self._arrow_client),
            GraphFilterResult(**JobClient.get_summary(self._arrow_client, job_id)),
        )

    def filter_async(
        self,
        G: Graph,
        graph_name: str,
        node_filter: str,
        relationship_filter: str,
        parameters: dict[str, Any] | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
    ) -> ProjectionJobHandle:
        """Kick off a graph filter operation and return a :class:`ProjectionJobHandle`.

        Unlike :meth:`filter`, this method does not block on completion.
        """
        config = ConfigConverter.convert_to_gds_config(
            from_graph_name=G.name(),
            graph_name=graph_name,
            node_filter=node_filter,
            relationship_filter=relationship_filter,
            parameters=parameters,
            concurrency=concurrency,
            job_id=job_id,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
        )

        started_job_id = JobClient.run_job(self._arrow_client, "v2/graph.project.filter", config)

        return ProjectionJobHandle(self._arrow_client, graph_name, started_job_id, TerminationFlag.create())

    def generate(
        self,
        graph_name: str,
        node_count: int,
        average_degree: float,
        *,
        relationship_distribution: str | None = "UNIFORM",
        relationship_seed: int | None = None,
        relationship_property: RelationshipPropertySpec | None = None,
        orientation: str | None = "NATURAL",
        aggregation: str | None = "NONE",
        allow_self_loops: bool | None = False,
        read_concurrency: int | None = None,
        job_id: str | None = None,
        sudo: bool = False,
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
            aggregation=aggregation,
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

    def generate_async(
        self,
        graph_name: str,
        node_count: int,
        average_degree: float,
        *,
        relationship_distribution: str | None = "UNIFORM",
        relationship_seed: int | None = None,
        relationship_property: RelationshipPropertySpec | None = None,
        orientation: str | None = "NATURAL",
        aggregation: str | None = "NONE",
        allow_self_loops: bool | None = False,
        read_concurrency: int | None = None,
        job_id: str | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
    ) -> ProjectionJobHandle:
        """Kick off a graph generation and return a :class:`ProjectionJobHandle`.

        Unlike :meth:`generate`, this method does not block on completion.
        """
        config = ConfigConverter.convert_to_gds_config(
            graph_name=graph_name,
            node_count=node_count,
            average_degree=average_degree,
            relationship_distribution=relationship_distribution,
            relationship_seed=relationship_seed,
            relationship_property=relationship_property.model_dump(by_alias=True) if relationship_property else None,
            orientation=orientation,
            aggregation=aggregation,
            allow_self_loops=allow_self_loops,
            read_concurrency=read_concurrency,
            job_id=job_id,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
        )

        started_job_id = JobClient.run_job(self._arrow_client, "v2/graph.generate", config)

        return ProjectionJobHandle(self._arrow_client, graph_name, started_job_id, TerminationFlag.create())

    def list(self, G: Graph | str | None = None) -> list[GraphInfoWithDegrees]:
        graph_name: str | None = None
        if isinstance(G, Graph):
            graph_name = G.name()
        elif isinstance(G, str):
            graph_name = G

        return self._graph_backend.list(graph_name)

    @property
    def export(self) -> GraphExportEndpoints:
        return GraphExportArrowEndpoints()

    @property
    def sample(self) -> GraphSamplingEndpoints:
        return GraphSamplingArrowEndpoints(self._arrow_client, show_progress=self._show_progress)

    @property
    def node_labels(self) -> NodeLabelEndpoints:
        write_client = self._write_protocol

        return NodeLabelArrowEndpoints(self._arrow_client, write_client, show_progress=self._show_progress)

    @property
    def node_properties(self) -> NodePropertiesEndpoints:
        return NodePropertiesArrowEndpoints(self._arrow_client, self._query_runner)

    @property
    def relationships(self) -> RelationshipsEndpoints:
        return RelationshipArrowEndpoints(
            self._arrow_client,
            self._write_protocol,
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
