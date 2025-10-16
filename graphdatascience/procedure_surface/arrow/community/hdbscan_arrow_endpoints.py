from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.hdbscan_endpoints import (
    HdbscanEndpoints,
    HdbscanMutateResult,
    HdbscanStatsResult,
    HdbscanWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper


class HdbscanArrowEndpoints(HdbscanEndpoints):
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
        node_property: str,
        mutate_property: str,
        *,
        leaf_size: int | None = None,
        samples: int | None = None,
        min_cluster_size: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        job_id: Any | None = None,
        username: str | None = None,
    ) -> HdbscanMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            node_property=node_property,
            leaf_size=leaf_size,
            samples=samples,
            min_cluster_size=min_cluster_size,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            log_progress=log_progress,
            sudo=sudo,
            job_id=job_id,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/community.hdbscan", config, mutate_property)

        return HdbscanMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        node_property: str,
        *,
        leaf_size: int | None = None,
        samples: int | None = None,
        min_cluster_size: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        job_id: Any | None = None,
        username: str | None = None,
    ) -> HdbscanStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            node_property=node_property,
            leaf_size=leaf_size,
            samples=samples,
            min_cluster_size=min_cluster_size,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            log_progress=log_progress,
            sudo=sudo,
            job_id=job_id,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_get_summary("v2/community.hdbscan", config)

        return HdbscanStatsResult(**result)

    def stream(
        self,
        G: GraphV2,
        node_property: str,
        *,
        leaf_size: int | None = None,
        samples: int | None = None,
        min_cluster_size: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        job_id: Any | None = None,
        username: str | None = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            node_property=node_property,
            leaf_size=leaf_size,
            samples=samples,
            min_cluster_size=min_cluster_size,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            log_progress=log_progress,
            sudo=sudo,
            job_id=job_id,
            username=username,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/community.hdbscan", G, config)

    def write(
        self,
        G: GraphV2,
        node_property: str,
        write_property: str,
        *,
        leaf_size: int | None = None,
        samples: int | None = None,
        min_cluster_size: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        write_concurrency: int | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        job_id: Any | None = None,
        username: str | None = None,
    ) -> HdbscanWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            node_property=node_property,
            leaf_size=leaf_size,
            samples=samples,
            min_cluster_size=min_cluster_size,
            relationship_types=relationship_types,
            node_labels=node_labels,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
            log_progress=log_progress,
            sudo=sudo,
            job_id=job_id,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/community.hdbscan",
            G,
            config,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )

        return HdbscanWriteResult(**result)

    def estimate(
        self,
        G: GraphV2,
        node_property: str,
        *,
        leaf_size: int | None = None,
        samples: int | None = None,
        min_cluster_size: int | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool | None = None,
        job_id: Any | None = None,
        username: str | None = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            node_property=node_property,
            leaf_size=leaf_size,
            samples=samples,
            min_cluster_size=min_cluster_size,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            log_progress=log_progress,
            sudo=sudo,
            job_id=job_id,
            username=username,
        )

        return self._node_property_endpoints.estimate("v2/community.hdbscan.estimate", G, config)
