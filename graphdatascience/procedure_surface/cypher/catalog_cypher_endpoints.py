from __future__ import annotations

from typing import Any, List, Optional, Tuple, Union

from graphdatascience.procedure_surface.api.catalog.graph_api import Graph
from graphdatascience.procedure_surface.api.catalog.graph_info import GraphInfo
from graphdatascience.procedure_surface.api.graph_with_result import GraphWithResult
from graphdatascience.procedure_surface.cypher.catalog.graph_backend_cypher import wrap_graph

from ...call_parameters import CallParameters
from ...query_runner.query_runner import QueryRunner
from ..api.base_result import BaseResult
from ..api.catalog_endpoints import (
    CatalogEndpoints,
    GraphFilterResult,
    GraphGenerationStats,
    RelationshipPropertySpec,
)
from ..api.graph_sampling_endpoints import GraphSamplingEndpoints
from ..utils.config_converter import ConfigConverter
from .catalog.node_label_cypher_endpoints import NodeLabelCypherEndpoints
from .catalog.node_properties_cypher_endpoints import NodePropertiesCypherEndpoints
from .catalog.relationship_cypher_endpoints import RelationshipCypherEndpoints
from .graph_sampling_cypher_endpoints import GraphSamplingCypherEndpoints


class CatalogCypherEndpoints(CatalogEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def list(self, G: Optional[Union[Graph, str]] = None) -> List[GraphInfo]:
        graph_name = G if isinstance(G, str) else G.name() if G is not None else None
        params = CallParameters(graphName=graph_name) if graph_name else CallParameters()

        result = self._query_runner.call_procedure(endpoint="gds.graph.list", params=params)
        return [GraphInfo(**row.to_dict()) for _, row in result.iterrows()]

    def drop(self, G: Union[Graph, str], fail_if_missing: Optional[bool] = None) -> Optional[GraphInfo]:
        graph_name = G if isinstance(G, str) else G.name()

        params = (
            CallParameters(graphName=graph_name, failIfMissing=fail_if_missing)
            if fail_if_missing is not None
            else CallParameters(graphName=graph_name)
        )

        result = self._query_runner.call_procedure(endpoint="gds.graph.drop", params=params)
        if len(result) > 0:
            return GraphInfo(**result.iloc[0].to_dict())
        else:
            return None

    def project(
        self,
        graph_name: str,
        node_projection: Optional[Union[str, List[str], dict[str, Any]]] = None,
        relationship_projection: Optional[Union[str, List[str], dict[str, Any]]] = None,
        node_properties: Optional[Union[str, List[str], dict[str, Any]]] = None,
        relationship_properties: Optional[Union[str, List[str], dict[str, Any]]] = None,
        read_concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        sudo: Optional[bool] = None,
        username: Optional[str] = None,
    ) -> Tuple[Graph, GraphProjectResult]:
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

        result = self._query_runner.call_procedure(endpoint="gds.graph.project", params=params).squeeze()
        list_result = GraphProjectResult(**result.to_dict())
        return wrap_graph(list_result.graph_name, self._query_runner), list_result

    def filter(
        self,
        G: Graph,
        graph_name: str,
        node_filter: str,
        relationship_filter: str,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
    ) -> GraphWithResult[GraphFilterResult]:
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

        result = self._query_runner.call_procedure(endpoint="gds.graph.filter", params=params).squeeze()
        return GraphWithResult(wrap_graph(graph_name, self._query_runner), GraphFilterResult(**result.to_dict()))

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

        result = self._query_runner.call_procedure(endpoint="gds.graph.generate", params=params).squeeze()
        return GraphWithResult(wrap_graph(graph_name, self._query_runner), GraphGenerationStats(**result.to_dict()))

    @property
    def sample(self) -> GraphSamplingEndpoints:
        return GraphSamplingCypherEndpoints(self._query_runner)

    @property
    def node_labels(self) -> NodeLabelCypherEndpoints:
        return NodeLabelCypherEndpoints(self._query_runner)

    @property
    def node_properties(self) -> NodePropertiesCypherEndpoints:
        return NodePropertiesCypherEndpoints(self._query_runner)

    @property
    def relationships(self) -> RelationshipCypherEndpoints:
        return RelationshipCypherEndpoints(self._query_runner)


class GraphProjectResult(BaseResult):
    graph_name: str
    node_count: int
    relationship_count: int
    project_millis: int
    node_projection: dict[str, Any]
    relationship_projection: dict[str, Any]
