from __future__ import annotations

from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pathfinding.steiner_tree_endpoints import (
    SteinerTreeEndpoints,
    SteinerTreeMutateResult,
    SteinerTreeStatsResult,
    SteinerTreeWriteResult,
)
from graphdatascience.procedure_surface.arrow.relationship_endpoints_helper import RelationshipEndpointsHelper
from graphdatascience.procedure_surface.arrow.stream_result_mapper import map_steiner_tree_stream_result


class SteinerTreeArrowEndpoints(SteinerTreeEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = False,
    ):
        self._endpoints_helper = RelationshipEndpointsHelper(
            arrow_client, write_back_client=write_back_client, show_progress=show_progress
        )

    def stream(
        self,
        G: GraphV2,
        source_node: int,
        target_nodes: list[int],
        relationship_weight_property: str | None = None,
        delta: float = 2.0,
        apply_rerouting: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        config = self._endpoints_helper.create_base_config(
            G,
            sourceNode=source_node,
            targetNodes=target_nodes,
            relationshipWeightProperty=relationship_weight_property,
            delta=delta,
            applyRerouting=apply_rerouting,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        result = self._endpoints_helper.run_job_and_stream("v2/pathfinding.steinerTree", G, config)
        map_steiner_tree_stream_result(result)
        return result

    def stats(
        self,
        G: GraphV2,
        source_node: int,
        target_nodes: list[int],
        relationship_weight_property: str | None = None,
        delta: float = 2.0,
        apply_rerouting: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> SteinerTreeStatsResult:
        config = self._endpoints_helper.create_base_config(
            G,
            sourceNode=source_node,
            targetNodes=target_nodes,
            relationshipWeightProperty=relationship_weight_property,
            delta=delta,
            applyRerouting=apply_rerouting,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        result = self._endpoints_helper.run_job_and_get_summary("v2/pathfinding.steinerTree", config)
        return SteinerTreeStatsResult(**result)

    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        mutate_property: str,
        source_node: int,
        target_nodes: list[int],
        relationship_weight_property: str | None = None,
        delta: float = 2.0,
        apply_rerouting: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> SteinerTreeMutateResult:
        config = self._endpoints_helper.create_base_config(
            G,
            sourceNode=source_node,
            targetNodes=target_nodes,
            relationshipWeightProperty=relationship_weight_property,
            delta=delta,
            applyRerouting=apply_rerouting,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        result = self._endpoints_helper.run_job_and_mutate(
            "v2/pathfinding.steinerTree",
            config,
            mutate_property=mutate_property,
            mutate_relationship_type=mutate_relationship_type,
        )

        return SteinerTreeMutateResult(**result)

    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        write_property: str,
        source_node: int,
        target_nodes: list[int],
        relationship_weight_property: str | None = None,
        delta: float = 2.0,
        apply_rerouting: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> SteinerTreeWriteResult:
        config = self._endpoints_helper.create_base_config(
            G,
            sourceNode=source_node,
            targetNodes=target_nodes,
            relationshipWeightProperty=relationship_weight_property,
            delta=delta,
            applyRerouting=apply_rerouting,
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
            "v2/pathfinding.steinerTree",
            G,
            config,
            relationship_type_overwrite=write_relationship_type,
            property_overwrites={write_property: write_property},
            write_concurrency=write_concurrency,
            concurrency=None,
        )

        return SteinerTreeWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        source_node: int,
        target_nodes: list[int],
        relationship_weight_property: str | None = None,
        delta: float = 2.0,
        apply_rerouting: bool = False,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        config = self._endpoints_helper.create_estimate_config(
            sourceNode=source_node,
            targetNodes=target_nodes,
            relationshipWeightProperty=relationship_weight_property,
            delta=delta,
            applyRerouting=apply_rerouting,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            username=username,
            concurrency=concurrency,
        )

        return self._endpoints_helper.estimate("v2/pathfinding.steinerTree.estimate", G, config)
