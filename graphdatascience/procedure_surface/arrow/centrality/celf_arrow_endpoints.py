from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.centrality.celf_endpoints import (
    CelfEndpoints,
    CelfMutateResult,
    CelfStatsResult,
    CelfWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper


class CelfArrowEndpoints(CelfEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client, write_back_client, show_progress=show_progress
        )

    def mutate(
        self,
        G: GraphV2,
        seed_set_size: int,
        mutate_property: str,
        propagation_probability: float | None = None,
        monte_carlo_simulations: int | None = None,
        random_seed: Any | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> CelfMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            seed_set_size=seed_set_size,
            propagation_probability=propagation_probability,
            monte_carlo_simulations=monte_carlo_simulations,
            random_seed=random_seed,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/centrality.celf", config, mutate_property)

        return CelfMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        seed_set_size: int,
        propagation_probability: float | None = None,
        monte_carlo_simulations: int | None = None,
        random_seed: Any | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> CelfStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            seed_set_size=seed_set_size,
            propagation_probability=propagation_probability,
            monte_carlo_simulations=monte_carlo_simulations,
            random_seed=random_seed,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/centrality.celf", config)

        return CelfStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        seed_set_size: int,
        propagation_probability: float | None = None,
        monte_carlo_simulations: int | None = None,
        random_seed: Any | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            seed_set_size=seed_set_size,
            propagation_probability=propagation_probability,
            monte_carlo_simulations=monte_carlo_simulations,
            random_seed=random_seed,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/centrality.celf", G, config)

    def write(
        self,
        G: GraphV2,
        seed_set_size: int,
        write_property: str,
        propagation_probability: float | None = None,
        monte_carlo_simulations: int | None = None,
        random_seed: Any | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> CelfWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            seed_set_size=seed_set_size,
            propagation_probability=propagation_probability,
            monte_carlo_simulations=monte_carlo_simulations,
            random_seed=random_seed,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/centrality.celf",
            G,
            config,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
            property_overwrites=write_property,
        )

        return CelfWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        seed_set_size: int,
        propagation_probability: float | None = None,
        monte_carlo_simulations: int | None = None,
        random_seed: Any | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
    ) -> EstimationResult:
        algo_config = self._node_property_endpoints.create_estimate_config(
            seed_set_size=seed_set_size,
            propagation_probability=propagation_probability,
            monte_carlo_simulations=monte_carlo_simulations,
            random_seed=random_seed,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
        )

        return self._node_property_endpoints.estimate("v2/centrality.celf.estimate", G, algo_config)
