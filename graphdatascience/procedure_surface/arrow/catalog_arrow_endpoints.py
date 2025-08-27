from __future__ import annotations

from typing import Any, List, Optional, Union
from uuid import uuid4

from graphdatascience import Graph, QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog_endpoints import (
    CatalogEndpoints,
    GraphFilterResult,
    GraphListResult,
)
from graphdatascience.procedure_surface.api.graph_sampling_endpoints import GraphSamplingEndpoints
from graphdatascience.procedure_surface.arrow.graph_sampling_arrow_endpoints import GraphSamplingArrowEndpoints
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.protocol.project_protocols import ProjectProtocol
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.session.dbms.protocol_resolver import ProtocolVersionResolver


class CatalogArrowEndpoints(CatalogEndpoints):
    GDS_REMOTE_PROJECTION_PROC_NAME = "gds.arrow.project"

    def __init__(self, arrow_client: AuthenticatedArrowClient, query_runner: Optional[QueryRunner] = None):
        self._arrow_client = arrow_client
        self._query_runner = query_runner
        if query_runner is not None:
            protocol_version = ProtocolVersionResolver(query_runner).resolve()
            self._project_protocol = ProjectProtocol.select(protocol_version)

    def list(self, G: Optional[Union[Graph, str]] = None) -> List[GraphListResult]:
        name = G if isinstance(G, str) else G.name() if G is not None else None

        payload = {"graphName": name} if G else {}

        result = self._arrow_client.do_action_with_retry("v2/graph.list", payload)

        return [GraphListResult(**x) for x in deserialize(result)]

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
    ) -> ProjectionResult:
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

        return ProjectionResult(**JobClient.get_summary(self._arrow_client, job_id))

    def drop(self, G: Union[Graph, str], fail_if_missing: Optional[bool] = None) -> Optional[GraphListResult]:
        graph_name = G if isinstance(G, str) else G.name()
        config = ConfigConverter.convert_to_gds_config(graph_name=graph_name, fail_if_missing=fail_if_missing)
        result = self._arrow_client.do_action_with_retry("v2/graph.drop", config)
        deserialized_results = deserialize(result)

        if len(deserialized_results) == 1:
            return GraphListResult(**deserialized_results[0])
        else:
            return None

    def filter(
        self,
        G: Graph,
        graph_name: str,
        node_filter: str,
        relationship_filter: str,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
    ) -> GraphFilterResult:
        config = ConfigConverter.convert_to_gds_config(
            from_graph_name=G.name(),
            graph_name=graph_name,
            node_filter=node_filter,
            relationship_filter=relationship_filter,
            concurrency=concurrency,
            job_id=job_id,
        )

        job_id = JobClient.run_job_and_wait(self._arrow_client, "v2/graph.project.filter", config)

        return GraphFilterResult(**JobClient.get_summary(self._arrow_client, job_id))

    @property
    def sample(self) -> GraphSamplingEndpoints:
        return GraphSamplingArrowEndpoints(self._arrow_client)

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
