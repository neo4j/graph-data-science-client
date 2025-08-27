from __future__ import annotations

from typing import Any, List, Optional

from ...call_parameters import CallParameters
from ...graph.graph_object import Graph
from ...query_runner.query_runner import QueryRunner
from ..api.graph_sampling_endpoints import GraphSamplingEndpoints, GraphSamplingResult
from ..utils.config_converter import ConfigConverter


class GraphSamplingCypherEndpoints(GraphSamplingEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def rwr(
        self,
        G: Graph,
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
    ) -> GraphSamplingResult:
        config = ConfigConverter.convert_to_gds_config(
            startNodes=start_nodes,
            restartProbability=restart_probability,
            samplingRatio=sampling_ratio,
            nodeLabelStratification=node_label_stratification,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        params = CallParameters(
            graph_name=graph_name,
            from_graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.graph.sample.rwr", params=params).squeeze()
        return GraphSamplingResult(**result.to_dict())

    def cnarw(
        self,
        G: Graph,
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
    ) -> GraphSamplingResult:
        config = ConfigConverter.convert_to_gds_config(
            startNodes=start_nodes,
            restartProbability=restart_probability,
            samplingRatio=sampling_ratio,
            nodeLabelStratification=node_label_stratification,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        params = CallParameters(
            graph_name=graph_name,
            from_graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.graph.sample.cnarw", params=params).squeeze()
        return GraphSamplingResult(**result.to_dict())
