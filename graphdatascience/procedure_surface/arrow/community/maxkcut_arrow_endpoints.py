from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.maxkcut_endpoints import (
    MaxKCutEndpoints,
    MaxKCutMutateResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper


class MaxKCutArrowEndpoints(MaxKCutEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = True,
    ):
        self._arrow_client = arrow_client
        self._write_back_client = write_back_client
        self._show_progress = show_progress
        self._node_property_endpoints = NodePropertyEndpointsHelper(arrow_client, write_back_client, show_progress)

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        concurrency: int | None = None,
        iterations: int | None = None,
        job_id: str | None = None,
        k: int | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        sudo: bool | None = False,
        username: str | None = None,
        vns_max_neighborhood_order: int | None = None,
    ) -> MaxKCutMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            iterations=iterations,
            job_id=job_id,
            k=k,
            log_progress=log_progress,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
            username=username,
            vns_max_neighborhood_order=vns_max_neighborhood_order,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/community.maxkcut", config, mutate_property)

        return MaxKCutMutateResult(**result)

    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        iterations: int | None = None,
        job_id: str | None = None,
        k: int | None = None,
        log_progress: bool = True,
        min_community_size: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        sudo: bool | None = False,
        username: str | None = None,
        vns_max_neighborhood_order: int | None = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            iterations=iterations,
            job_id=job_id,
            k=k,
            log_progress=log_progress,
            min_community_size=min_community_size,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
            username=username,
            vns_max_neighborhood_order=vns_max_neighborhood_order,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/community.maxkcut", G, config)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        concurrency: int | None = None,
        iterations: int | None = None,
        k: int | None = None,
        node_labels: list[str] = ALL_LABELS,
        random_seed: int | None = None,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        vns_max_neighborhood_order: int | None = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            concurrency=concurrency,
            iterations=iterations,
            k=k,
            node_labels=node_labels,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            vns_max_neighborhood_order=vns_max_neighborhood_order,
        )

        return self._node_property_endpoints.estimate("v2/community.maxkcut.estimate", G, config)
