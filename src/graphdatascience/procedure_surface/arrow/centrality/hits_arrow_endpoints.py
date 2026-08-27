from __future__ import annotations

from collections import OrderedDict
from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.centrality.hits_endpoints import (
    HitsEndpoints,
    HitsMutateResult,
    HitsStatsResult,
    HitsWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol

HITS_ENDPOINT = "v2/centrality.hits"
HITS_ESTIMATE_ENDPOINT = "v2/centrality.hits.estimate"


class HitsArrowEndpoints(HitsEndpoints):
    """Arrow-based implementation of the HITS algorithm endpoints."""

    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client, write_protocol, show_progress=show_progress
        )

    def compute(
        self,
        G: Graph,
        *,
        hits_iterations: int = 20,
        auth_property: str = "auth",
        hub_property: str = "hub",
        partitioning: str = "AUTO",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> JobHandle:
        config = self._node_property_endpoints.create_base_config(
            G,
            hits_iterations=hits_iterations,
            auth_property=auth_property,
            hub_property=hub_property,
            partitioning=partitioning,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )
        return self._node_property_endpoints.run_job(G, HITS_ENDPOINT, config)

    def mutate(
        self,
        G: Graph,
        *,
        mutate_property: str = "",
        hits_iterations: int = 20,
        auth_property: str = "auth",
        hub_property: str = "hub",
        partitioning: str = "AUTO",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> HitsMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            hits_iterations=hits_iterations,
            auth_property=auth_property,
            hub_property=hub_property,
            partitioning=partitioning,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        # HITS produces two node properties (hub + auth). They are stored under
        # <hub_property|auth_property> + mutate_property, matching local GDS.
        mutate_property_overwrites: OrderedDict[str, str] = OrderedDict(
            [
                (hub_property, hub_property + mutate_property),
                (auth_property, auth_property + mutate_property),
            ]
        )

        result = self._node_property_endpoints.run_job_and_mutate_multiple(
            HITS_ENDPOINT, config, mutate_property_overwrites
        )

        return HitsMutateResult(**result)

    def stats(
        self,
        G: Graph,
        *,
        hits_iterations: int = 20,
        partitioning: str = "AUTO",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> HitsStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            hits_iterations=hits_iterations,
            partitioning=partitioning,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary(HITS_ENDPOINT, config)

        return HitsStatsResult(**computation_result)

    def stream(
        self,
        G: Graph,
        *,
        hits_iterations: int = 20,
        auth_property: str = "auth",
        hub_property: str = "hub",
        partitioning: str = "AUTO",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            hits_iterations=hits_iterations,
            auth_property=auth_property,
            hub_property=hub_property,
            partitioning=partitioning,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        return self._node_property_endpoints.run_job_and_stream(HITS_ENDPOINT, G, config)

    def write(
        self,
        G: Graph,
        *,
        write_property: str = "",
        hits_iterations: int = 20,
        auth_property: str = "auth",
        hub_property: str = "hub",
        partitioning: str = "AUTO",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> HitsWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            hits_iterations=hits_iterations,
            auth_property=auth_property,
            hub_property=hub_property,
            partitioning=partitioning,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        # HITS writes two node properties (hub + auth), stored under
        # <hub_property|auth_property> + write_property, matching local GDS.
        property_overwrites = {
            hub_property: hub_property + write_property,
            auth_property: auth_property + write_property,
        }

        result = self._node_property_endpoints.run_job_and_write(
            HITS_ENDPOINT,
            G,
            config,
            property_overwrites=property_overwrites,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )

        if "propertiesWritten" in result:
            result["nodePropertiesWritten"] = result.pop("propertiesWritten")

        return HitsWriteResult(**result)

    def estimate(
        self,
        G: Graph | dict[str, Any],
        *,
        hits_iterations: int = 20,
        partitioning: str = "AUTO",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            hits_iterations=hits_iterations,
            partitioning=partitioning,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
        )
        return self._node_property_endpoints.estimate(HITS_ESTIMATE_ENDPOINT, G, config)
