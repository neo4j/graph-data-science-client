from __future__ import annotations

from typing import Any

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.graph_sampling_endpoints import (
    GraphSamplingEndpoints,
    GraphSamplingResult,
    GraphWithSamplingResult,
)
from graphdatascience.procedure_surface.cypher.catalog.graph_backend_cypher import get_graph

from ....call_parameters import CallParameters
from ....query_runner.query_runner import QueryRunner
from ...utils.config_converter import ConfigConverter


class GraphSamplingCypherEndpoints(GraphSamplingEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def rwr(
        self,
        G: GraphV2,
        graph_name: str,
        start_nodes: list[int] | None = None,
        restart_probability: float | None = None,
        sampling_ratio: float | None = None,
        node_label_stratification: bool | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
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

        result = self._query_runner.call_procedure(
            endpoint="gds.graph.sample.rwr", params=params, logging=log_progress
        ).squeeze()
        return GraphWithSamplingResult(
            get_graph(graph_name, self._query_runner),
            GraphSamplingResult(**result.to_dict()),
        )

    def cnarw(
        self,
        G: GraphV2,
        graph_name: str,
        start_nodes: list[int] | None = None,
        restart_probability: float | None = None,
        sampling_ratio: float | None = None,
        node_label_stratification: bool | None = None,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
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

        result = self._query_runner.call_procedure(
            endpoint="gds.graph.sample.cnarw", params=params, logging=log_progress
        ).squeeze()
        return GraphWithSamplingResult(
            get_graph(graph_name, self._query_runner),
            GraphSamplingResult(**result.to_dict()),
        )
