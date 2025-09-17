from __future__ import annotations

from typing import Any, List, Optional, Union
from uuid import uuid4

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.graph_info import GraphInfo, GraphInfoWithDegrees
from graphdatascience.procedure_surface.api.catalog_endpoints import (
    CatalogEndpoints,
    GraphFilterResult,
    GraphGenerationStats,
    ProjectionResult,
    RelationshipPropertySpec,
)
from graphdatascience.procedure_surface.api.graph_sampling_endpoints import GraphSamplingEndpoints
from graphdatascience.procedure_surface.api.graph_with_result import GraphWithResult
from graphdatascience.procedure_surface.arrow.catalog.graph_backend_arrow import wrap_graph
from graphdatascience.procedure_surface.arrow.catalog.graph_ops_arrow import GraphOpsArrow
from graphdatascience.procedure_surface.arrow.catalog.node_label_arrow_endpoints import NodeLabelArrowEndpoints
from graphdatascience.procedure_surface.arrow.catalog.node_properties_arrow_endpoints import (
    NodePropertiesArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.catalog.relationship_arrow_endpoints import RelationshipArrowEndpoints
from graphdatascience.procedure_surface.arrow.graph_sampling_arrow_endpoints import GraphSamplingArrowEndpoints
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.protocol.project_protocols import ProjectProtocol
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.session.dbms.protocol_resolver import ProtocolVersionResolver


class CatalogArrowEndpoints(CatalogEndpoints):
    GDS_REMOTE_PROJECTION_PROC_NAME = "gds.arrow.project"

    def __init__(self, arrow_client: AuthenticatedArrowClient, query_runner: Optional[QueryRunner] = None):
        self._arrow_client = arrow_client
        self._query_runner = query_runner
        self._graph_backend = GraphOpsArrow(arrow_client)
        if query_runner is not None:
            protocol_version = ProtocolVersionResolver(query_runner).resolve()
            self._project_protocol = ProjectProtocol.select(protocol_version)

    def list(self, G: Optional[Union[GraphV2, str]] = None) -> List[GraphInfoWithDegrees]:
        graph_name: Optional[str] = None
        if isinstance(G, GraphV2):
            graph_name = G.name()
        elif isinstance(G, str):
            graph_name = G

        return self._graph_backend.list(graph_name)

    def project(
        self,
        graph_name: str,
        query: str,
        job_id: Optional[str] = None,
        concurrency: int = 4,
        undirected_relationship_types: Optional[List[str]] = None,
        inverse_indexed_relationship_types: Optional[List[str]] = None,
        batch_size: Optional[int] = None,
        logging: bool = True,
    ) -> GraphWithResult[ProjectionResult]:
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
            logging,
        )

        job_result = ProjectionResult(**JobClient.get_summary(self._arrow_client, job_id))

        return GraphWithResult(wrap_graph(graph_name, self._arrow_client), job_result)

    def drop(self, G: Union[GraphV2, str], fail_if_missing: bool = True) -> Optional[GraphInfo]:
        graph_name = G.name() if isinstance(G, GraphV2) else G

        return self._graph_backend.drop(graph_name, fail_if_missing)

    def filter(
        self,
        G: GraphV2,
        graph_name: str,
        node_filter: str,
        relationship_filter: str,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
    ) -> GraphWithResult[GraphFilterResult]:
        config = ConfigConverter.convert_to_gds_config(
            from_graph_name=G.name(),
            graph_name=graph_name,
            node_filter=node_filter,
            relationship_filter=relationship_filter,
            concurrency=concurrency,
            job_id=job_id,
        )

        job_id = JobClient.run_job_and_wait(self._arrow_client, "v2/graph.project.filter", config)

        return GraphWithResult(
            wrap_graph(graph_name, self._arrow_client),
            GraphFilterResult(**JobClient.get_summary(self._arrow_client, job_id)),
        )

    def generate(
        self,
        graph_name: str,
        node_count: int,
        average_degree: float,
        *,
        relationship_distribution: Optional[str] = None,
        relationship_seed: Optional[int] = None,
        relationship_property: Optional[RelationshipPropertySpec] = None,
        orientation: Optional[str] = None,
        allow_self_loops: Optional[bool] = None,
        read_concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
    ) -> GraphWithResult[GraphGenerationStats]:
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

        job_id = JobClient.run_job_and_wait(self._arrow_client, "v2/graph.generate", config)

        return GraphWithResult(
            wrap_graph(graph_name, self._arrow_client),
            GraphGenerationStats(**JobClient.get_summary(self._arrow_client, job_id)),
        )

    @property
    def sample(self) -> GraphSamplingEndpoints:
        return GraphSamplingArrowEndpoints(self._arrow_client)

    @property
    def node_labels(self) -> NodeLabelArrowEndpoints:
        write_client = RemoteWriteBackClient(self._arrow_client, self._query_runner) if self._query_runner else None

        return NodeLabelArrowEndpoints(self._arrow_client, write_client)

    @property
    def node_properties(self) -> NodePropertiesArrowEndpoints:
        return NodePropertiesArrowEndpoints(self._arrow_client, self._query_runner)

    @property
    def relationships(self) -> RelationshipArrowEndpoints:
        return RelationshipArrowEndpoints(
            self._arrow_client,
            RemoteWriteBackClient(self._arrow_client, self._query_runner) if self._query_runner else None,
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
