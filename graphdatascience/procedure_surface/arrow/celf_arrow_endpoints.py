from typing import Any, List, Optional, Union

from pandas import DataFrame

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from ...graph.graph_object import Graph
from ..api.celf_endpoints import CelfEndpoints, CelfMutateResult, CelfStatsResult, CelfWriteResult
from ..api.estimation_result import EstimationResult
from .node_property_endpoints import NodePropertyEndpoints


class CelfArrowEndpoints(CelfEndpoints):
    def __init__(
        self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[RemoteWriteBackClient] = None
    ):
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client, write_back_client)

    def mutate(
        self,
        G: Graph,
        seed_set_size: int,
        mutate_property: str,
        propagation_probability: Optional[float] = None,
        monte_carlo_simulations: Optional[int] = None,
        random_seed: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
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

        result = self._node_property_endpoints.run_job_and_mutate("v2/centrality.celf", G, config, mutate_property)

        return CelfMutateResult(**result)

    def stats(
        self,
        G: Graph,
        seed_set_size: int,
        propagation_probability: Optional[float] = None,
        monte_carlo_simulations: Optional[int] = None,
        random_seed: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
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

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/centrality.celf", G, config)

        return CelfStatsResult(**computation_result)

    def stream(
        self,
        G: Graph,
        seed_set_size: int,
        propagation_probability: Optional[float] = None,
        monte_carlo_simulations: Optional[int] = None,
        random_seed: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
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
        G: Graph,
        seed_set_size: int,
        write_property: str,
        propagation_probability: Optional[float] = None,
        monte_carlo_simulations: Optional[int] = None,
        random_seed: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        write_concurrency: Optional[int] = None,
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
            "v2/centrality.celf", G, config, write_concurrency=write_concurrency, concurrency=concurrency
        )

        return CelfWriteResult(**result)

    def estimate(
        self,
        G: Union[Graph, dict[str, Any]],
        seed_set_size: int,
        propagation_probability: Optional[float] = None,
        monte_carlo_simulations: Optional[int] = None,
        random_seed: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
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
