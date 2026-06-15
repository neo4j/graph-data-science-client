from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.community.local_clustering_coefficient_endpoints import (
    LocalClusteringCoefficientEndpoints,
    LocalClusteringCoefficientMutateResult,
    LocalClusteringCoefficientStatsResult,
    LocalClusteringCoefficientWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol


class LocalClusteringCoefficientArrowEndpoints(LocalClusteringCoefficientEndpoints):
    def __init__(
        self,
        client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            client,
            write_protocol,
            show_progress,
        )

    def compute(
        self,
        G: Graph,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
    ) -> JobHandle:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            triangle_count_property=triangle_count_property,
            username=username,
        )
        return self._node_property_endpoints.run_job(G, "v2/community.localClusteringCoefficient", config)

    def mutate(
        self,
        G: Graph,
        *,
        mutate_property: str,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
    ) -> LocalClusteringCoefficientMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            triangle_count_property=triangle_count_property,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_mutate(
            "v2/community.localClusteringCoefficient", config, mutate_property
        )

        return LocalClusteringCoefficientMutateResult(**result)

    def stats(
        self,
        G: Graph,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
    ) -> LocalClusteringCoefficientStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            triangle_count_property=triangle_count_property,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_get_summary(
            "v2/community.localClusteringCoefficient",
            config,
        )

        return LocalClusteringCoefficientStatsResult(**result)

    def stream(
        self,
        G: Graph,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            triangle_count_property=triangle_count_property,
            username=username,
        )

        return self._node_property_endpoints.run_job_and_stream(
            "v2/community.localClusteringCoefficient",
            G,
            config,
        )

    def write(
        self,
        G: Graph,
        *,
        write_property: str,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> LocalClusteringCoefficientWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            write_property=write_property,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            triangle_count_property=triangle_count_property,
            username=username,
            write_concurrency=write_concurrency,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/community.localClusteringCoefficient",
            G,
            config,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )

        return LocalClusteringCoefficientWriteResult(**result)

    def estimate(
        self,
        G: Graph,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        sudo: bool | None = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            triangle_count_property=triangle_count_property,
            username=username,
        )

        result = self._node_property_endpoints.estimate(
            "v2/community.localClusteringCoefficient.estimate",
            G,
            config,
        )

        return result
