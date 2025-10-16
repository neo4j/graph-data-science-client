import pandas as pd

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.local_clustering_coefficient_endpoints import (
    LocalClusteringCoefficientEndpoints,
    LocalClusteringCoefficientMutateResult,
    LocalClusteringCoefficientStatsResult,
    LocalClusteringCoefficientWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints


class LocalClusteringCoefficientArrowEndpoints(LocalClusteringCoefficientEndpoints):
    def __init__(
        self,
        client: AuthenticatedArrowClient,
        remote_write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpoints(
            client,
            remote_write_back_client,
            show_progress,
        )

    def mutate(
        self,
        G: GraphV2,
        *,
        mutate_property: str,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
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
            "v2/community.localClusteringCoefficient", G, config, mutate_property
        )

        return LocalClusteringCoefficientMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
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
            G,
            config,
        )

        return LocalClusteringCoefficientStatsResult(**result)

    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
    ) -> pd.DataFrame:
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
        G: GraphV2,
        *,
        write_property: str,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
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
            write_concurrency,
            concurrency,
            write_property,
        )

        return LocalClusteringCoefficientWriteResult(**result)

    def estimate(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] | None = None,
        relationship_types: list[str] | None = None,
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
