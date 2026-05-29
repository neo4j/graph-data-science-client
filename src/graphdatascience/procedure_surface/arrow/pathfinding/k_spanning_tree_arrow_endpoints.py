from __future__ import annotations

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.pathfinding.k_spanning_tree_endpoints import (
    KSpanningTreeEndpoints,
    KSpanningTreeWriteResult,
)
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper
from graphdatascience.query_runner.protocol.write_protocols import WriteProtocol


class KSpanningTreeArrowEndpoints(KSpanningTreeEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = False,
    ):
        self._endpoints_helper = NodePropertyEndpointsHelper(
            arrow_client, write_protocol=write_protocol, show_progress=show_progress
        )

    def compute(
        self,
        G: GraphV2,
        k: int,
        source_node: int,
        *,
        relationship_weight_property: str | None = None,
        objective: str = "minimum",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> JobHandle:
        config = self._endpoints_helper.create_base_config(
            G,
            k=k,
            sourceNode=source_node,
            relationshipWeightProperty=relationship_weight_property,
            objective=objective,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )
        return self._endpoints_helper.run_job(G, "v2/pathfinding.kSpanningTree", config)

    def write(
        self,
        G: GraphV2,
        k: int,
        write_property: str,
        source_node: int,
        relationship_weight_property: str | None = None,
        objective: str = "minimum",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> KSpanningTreeWriteResult:
        config = self._endpoints_helper.create_base_config(
            G,
            k=k,
            sourceNode=source_node,
            relationshipWeightProperty=relationship_weight_property,
            objective=objective,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
            writeConcurrency=write_concurrency,
        )

        result = self._endpoints_helper.run_job_and_write(
            "v2/pathfinding.kSpanningTree",
            G,
            config,
            property_overwrites={write_property: write_property},
            write_concurrency=write_concurrency,
            concurrency=None,
        )

        return KSpanningTreeWriteResult(**result)
