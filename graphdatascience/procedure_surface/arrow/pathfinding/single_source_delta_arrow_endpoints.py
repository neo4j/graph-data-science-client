from __future__ import annotations

from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pathfinding.single_source_delta_endpoints import (
    DeltaSteppingMutateResult,
    DeltaSteppingStatsResult,
    DeltaSteppingWriteResult,
    SingleSourceDeltaEndpoints,
)
from graphdatascience.procedure_surface.arrow.relationship_endpoints_helper import RelationshipEndpointsHelper
from graphdatascience.procedure_surface.arrow.stream_result_mapper import map_shortest_path_stream_result


class DeltaSteppingArrowEndpoints(SingleSourceDeltaEndpoints):
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
        delta: float = 2.0,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        config = self._endpoints_helper.create_base_config(
            G,
            sourceNode=source_node,
            delta=delta,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        result = self._endpoints_helper.run_job_and_stream("v2/pathfinding.singleSource.deltaStepping", G, config)
        map_shortest_path_stream_result(result)

        return result

    def stats(
        self,
        G: GraphV2,
        source_node: int,
        delta: float = 2.0,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DeltaSteppingStatsResult:
        config = self._endpoints_helper.create_base_config(
            G,
            sourceNode=source_node,
            delta=delta,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        result = self._endpoints_helper.run_job_and_get_result(
            "v2/pathfinding.singleSource.deltaStepping",
            G,
            config,
            ["preProcessingMillis", "computeMillis", "postProcessingMillis", "configuration"],
        )

        return DeltaSteppingStatsResult(**result)

    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        source_node: int,
        delta: float = 2.0,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DeltaSteppingMutateResult:
        config = self._endpoints_helper.create_base_config(
            G,
            sourceNode=source_node,
            delta=delta,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        result = self._endpoints_helper.run_job_and_mutate(
            "v2/pathfinding.singleSource.deltaStepping",
            config,
            mutate_property=None,
            mutate_relationship_type=mutate_relationship_type,
        )

        return DeltaSteppingMutateResult(**result)

    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        source_node: int,
        delta: float = 2.0,
        write_node_ids: bool = False,
        write_costs: bool = False,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> DeltaSteppingWriteResult:
        config = self._endpoints_helper.create_base_config(
            G,
            sourceNode=source_node,
            delta=delta,
            writeNodeIds=write_node_ids,
            writeCosts=write_costs,
            relationshipWeightProperty=relationship_weight_property,
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
            "v2/pathfinding.singleSource.deltaStepping",
            G,
            config,
            relationship_type_overwrite=write_relationship_type,
            property_overwrites=None,
            write_concurrency=write_concurrency,
            concurrency=None,
        )

        return DeltaSteppingWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        source_node: int,
        delta: float = 2.0,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        config = self._endpoints_helper.create_estimate_config(
            sourceNode=source_node,
            delta=delta,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            username=username,
            concurrency=concurrency,
        )

        return self._endpoints_helper.estimate("v2/pathfinding.singleSource.deltaStepping.estimate", G, config)
