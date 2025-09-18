from __future__ import annotations

from typing import Any, List, Optional

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.catalog.graph_backend_cypher import get_graph

from ...call_parameters import CallParameters
from ...query_runner.query_runner import QueryRunner
from ..api.graph_sampling_endpoints import GraphSamplingEndpoints, GraphSamplingResult, GraphWithSamplingResult
from ..utils.config_converter import ConfigConverter


class GraphSamplingCypherEndpoints(GraphSamplingEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def rwr(
        self,
        G: GraphV2,
        graph_name: str,
        start_nodes: Optional[List[int]] = None,
        restart_probability: Optional[float] = None,
        sampling_ratio: Optional[float] = None,
        node_label_stratification: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> GraphWithSamplingResult:
        config = ConfigConverter.convert_to_gds_config(
            start_nodes=start_nodes,
            restart_probability=restart_probability,
            sampling_ratio=sampling_ratio,
            node_label_stratification=node_label_stratification,
            relationship_weight_property=relationship_weight_property,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        params = CallParameters(
            graph_name=graph_name,
            from_graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.graph.sample.rwr", params=params).squeeze()
        return GraphWithSamplingResult(
            get_graph(graph_name, self._query_runner),
            GraphSamplingResult(**result.to_dict()),
        )

    def cnarw(
        self,
        G: GraphV2,
        graph_name: str,
        start_nodes: Optional[List[int]] = None,
        restart_probability: Optional[float] = None,
        sampling_ratio: Optional[float] = None,
        node_label_stratification: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> GraphWithSamplingResult:
        config = ConfigConverter.convert_to_gds_config(
            start_nodes=start_nodes,
            restart_probability=restart_probability,
            sampling_ratio=sampling_ratio,
            node_label_stratification=node_label_stratification,
            relationship_weight_property=relationship_weight_property,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        params = CallParameters(
            graph_name=graph_name,
            from_graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.graph.sample.cnarw", params=params).squeeze()
        return GraphWithSamplingResult(
            get_graph(graph_name, self._query_runner),
            GraphSamplingResult(**result.to_dict()),
        )
