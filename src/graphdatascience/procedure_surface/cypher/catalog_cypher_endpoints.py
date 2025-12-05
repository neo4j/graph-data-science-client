from __future__ import annotations

import builtins
from types import TracebackType
from typing import Any, NamedTuple, Type

from graphdatascience.arrow_client.v1.gds_arrow_client import GdsArrowClient
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
from graphdatascience.procedure_surface.cypher.catalog.graph_backend_cypher import get_graph
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

from ...call_parameters import CallParameters
from ..api.base_result import BaseResult
from ..utils.config_converter import ConfigConverter
from .catalog.graph_sampling_cypher_endpoints import GraphSamplingCypherEndpoints
from .catalog.node_label_cypher_endpoints import NodeLabelCypherEndpoints
from .catalog.node_properties_cypher_endpoints import NodePropertiesCypherEndpoints
from .catalog.relationship_cypher_endpoints import RelationshipCypherEndpoints


class CatalogCypherEndpoints(CatalogEndpoints):
    def __init__(self, cypher_runner: Neo4jQueryRunner, arrow_client: GdsArrowClient | None = None):
        self.cypher_runner = cypher_runner
        self._arrow_client = arrow_client

    def list(self, G: GraphV2 | str | None = None) -> list[GraphInfoWithDegrees]:
        graph_name = G if isinstance(G, str) else G.name() if G is not None else None
        params = CallParameters(graphName=graph_name) if graph_name else CallParameters()

        result = self.cypher_runner.call_procedure(endpoint="gds.graph.list", params=params)
        return [GraphInfoWithDegrees(**row.to_dict()) for _, row in result.iterrows()]

    def drop(self, G: GraphV2 | str, fail_if_missing: bool = True) -> GraphInfo | None:
        graph_name = G if isinstance(G, str) else G.name()

        params = (
            CallParameters(graphName=graph_name, failIfMissing=fail_if_missing)
            if fail_if_missing is not None
            else CallParameters(graphName=graph_name)
        )

        result = self.cypher_runner.call_procedure(endpoint="gds.graph.drop", params=params)
        if len(result) > 0:
            return GraphInfo(**result.iloc[0].to_dict())
        else:
            return None

    def project(
        self,
        graph_name: str,
        node_projection: str | builtins.list[str] | dict[str, Any] | None = None,
        relationship_projection: str | builtins.list[str] | dict[str, Any] | None = None,
        node_properties: str | builtins.list[str] | dict[str, Any] | None = None,
        relationship_properties: str | builtins.list[str] | dict[str, Any] | None = None,
        read_concurrency: int | None = None,
        job_id: str | None = None,
        sudo: bool = False,
        username: str | None = None,
        log_progress: bool = True,
    ) -> GraphWithProjectResult:
        config = ConfigConverter.convert_to_gds_config(
            nodeProperties=node_properties,
            relationshipProperties=relationship_properties,
            jobId=job_id,
            sudo=sudo,
            username=username,
            readConcurrency=read_concurrency,
        )

        params = CallParameters(
            graphName=graph_name,
            nodeProjection=node_projection,
            relationshipProjection=relationship_projection,
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self.cypher_runner.call_procedure(
            endpoint="gds.graph.project", params=params, logging=log_progress
        ).squeeze()
        project_result = GraphProjectResult(**result.to_dict())
        return GraphWithProjectResult(get_graph(project_result.graph_name, self.cypher_runner), project_result)

    def filter(
        self,
        G: GraphV2,
        graph_name: str,
        node_filter: str,
        relationship_filter: str,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
    ) -> GraphWithFilterResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            jobId=job_id,
        )

        params = CallParameters(
            graph_name=graph_name,
            from_graph_name=G.name(),
            node_filter=node_filter,
            relationship_filter=relationship_filter,
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self.cypher_runner.call_procedure(
            endpoint="gds.graph.filter", params=params, logging=log_progress
        ).squeeze()
        return GraphWithFilterResult(get_graph(graph_name, self.cypher_runner), GraphFilterResult(**result.to_dict()))

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
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
    ) -> GraphWithGenerationStats:
        config = ConfigConverter.convert_to_gds_config(
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

        params = CallParameters(
            graph_name=graph_name,
            node_count=node_count,
            average_degree=average_degree,
            config=config,
        )

        params.ensure_job_id_in_config()

        result = self.cypher_runner.call_procedure(
            endpoint="gds.graph.generate", params=params, logging=log_progress
        ).squeeze()
        return GraphWithGenerationStats(
            get_graph(graph_name, self.cypher_runner), GraphGenerationStats(**result.to_dict())
        )

    @property
    def sample(self) -> GraphSamplingEndpoints:
        return GraphSamplingCypherEndpoints(self.cypher_runner)

    @property
    def node_labels(self) -> NodeLabelCypherEndpoints:
        return NodeLabelCypherEndpoints(self.cypher_runner)

    @property
    def node_properties(self) -> NodePropertiesCypherEndpoints:
        return NodePropertiesCypherEndpoints(self.cypher_runner, self._arrow_client)

    @property
    def relationships(self) -> RelationshipCypherEndpoints:
        return RelationshipCypherEndpoints(self.cypher_runner, self._arrow_client)


class GraphProjectResult(BaseResult):
    graph_name: str
    node_count: int
    relationship_count: int
    project_millis: int
    node_projection: dict[str, Any]
    relationship_projection: dict[str, Any]


class GraphWithProjectResult(NamedTuple):
    graph: GraphV2
    result: GraphProjectResult

    def __enter__(self) -> GraphV2:
        return self.graph

    def __exit__(
        self,
        exception_type: Type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.graph.drop()
