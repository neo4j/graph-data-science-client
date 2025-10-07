from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.maxkcut_endpoints import (
    MaxKCutEndpoints,
    MaxKCutMutateResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpoints


class MaxKCutArrowEndpoints(MaxKCutEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: Optional[RemoteWriteBackClient] = None,
        show_progress: bool = True,
    ):
        self._arrow_client = arrow_client
        self._write_back_client = write_back_client
        self._show_progress = show_progress
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client, write_back_client, show_progress)

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        concurrency: Optional[int] = None,
        iterations: Optional[int] = None,
        job_id: Optional[str] = None,
        k: Optional[int] = None,
        log_progress: bool = True,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
        vns_max_neighborhood_order: Optional[int] = None,
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

        result = self._node_property_endpoints.run_job_and_mutate("v2/community.maxkcut", G, config, mutate_property)

        return MaxKCutMutateResult(**result)

    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: Optional[int] = None,
        iterations: Optional[int] = None,
        job_id: Optional[str] = None,
        k: Optional[int] = None,
        log_progress: bool = True,
        min_community_size: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
        vns_max_neighborhood_order: Optional[int] = None,
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
        G: Union[GraphV2, dict[str, Any]],
        *,
        concurrency: Optional[int] = None,
        iterations: Optional[int] = None,
        k: Optional[int] = None,
        node_labels: Optional[List[str]] = None,
        random_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        relationship_weight_property: Optional[str] = None,
        vns_max_neighborhood_order: Optional[int] = None,
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
