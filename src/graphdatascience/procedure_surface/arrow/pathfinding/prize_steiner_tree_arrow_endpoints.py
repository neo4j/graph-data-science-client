from __future__ import annotations

from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.pathfinding.prize_steiner_tree_endpoints import (
    PrizeSteinerTreeEndpoints,
    PrizeSteinerTreeMutateResult,
    PrizeSteinerTreeStatsResult,
    PrizeSteinerTreeWriteResult,
)
from graphdatascience.procedure_surface.arrow.relationship_endpoints_helper import RelationshipEndpointsHelper
from graphdatascience.procedure_surface.arrow.stream_result_mapper import map_steiner_tree_stream_result
from graphdatascience.query_runner.protocol.write_protocols import WriteProtocol


class PrizeSteinerTreeArrowEndpoints(PrizeSteinerTreeEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = False,
    ):
        self._endpoints_helper = RelationshipEndpointsHelper(
            arrow_client, write_protocol=write_protocol, show_progress=show_progress
        )

    def compute(
        self,
        G: Graph,
        prize_property: str,
        *,
        relationship_weight_property: str | None = None,
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
            prizeProperty=prize_property,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )
        return self._endpoints_helper.run_job(G, "v2/pathfinding.prizeSteinerTree", config)

    def stream(
        self,
        G: Graph,
        prize_property: str,
        relationship_weight_property: str | None = None,
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
            prizeProperty=prize_property,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        result = self._endpoints_helper.run_job_and_stream("v2/pathfinding.prizeSteinerTree", G, config)
        map_steiner_tree_stream_result(result)
        return result

    def stats(
        self,
        G: Graph,
        prize_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> PrizeSteinerTreeStatsResult:
        config = self._endpoints_helper.create_base_config(
            G,
            prizeProperty=prize_property,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        result = self._endpoints_helper.run_job_and_get_summary("v2/pathfinding.prizeSteinerTree", config)
        return PrizeSteinerTreeStatsResult(**result)

    def mutate(
        self,
        G: Graph,
        mutate_relationship_type: str,
        mutate_property: str,
        prize_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> PrizeSteinerTreeMutateResult:
        config = self._endpoints_helper.create_base_config(
            G,
            prizeProperty=prize_property,
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
            "v2/pathfinding.prizeSteinerTree",
            config,
            mutate_property=mutate_property,
            mutate_relationship_type=mutate_relationship_type,
        )

        return PrizeSteinerTreeMutateResult(**result)

    def write(
        self,
        G: Graph,
        write_relationship_type: str,
        write_property: str,
        prize_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> PrizeSteinerTreeWriteResult:
        config = self._endpoints_helper.create_base_config(
            G,
            prizeProperty=prize_property,
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
            "v2/pathfinding.prizeSteinerTree",
            G,
            config,
            relationship_type_overwrite=write_relationship_type,
            property_overwrites={write_property: write_property},
            write_concurrency=write_concurrency,
            concurrency=None,
        )

        return PrizeSteinerTreeWriteResult(**result)

    def estimate(
        self,
        G: Graph | dict[str, Any],
        prize_property: str,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        config = self._endpoints_helper.create_estimate_config(
            prizeProperty=prize_property,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            username=username,
            concurrency=concurrency,
        )

        return self._endpoints_helper.estimate("v2/pathfinding.prizeSteinerTree.estimate", G, config)
