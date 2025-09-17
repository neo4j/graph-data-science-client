from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from ..api.degree_endpoints import DegreeEndpoints, DegreeMutateResult, DegreeStatsResult, DegreeWriteResult
from ..api.estimation_result import EstimationResult
from .node_property_endpoints import NodePropertyEndpoints


class DegreeArrowEndpoints(DegreeEndpoints):
    def __init__(
        self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[RemoteWriteBackClient] = None
    ):
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client, write_back_client)

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        orientation: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DegreeMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            orientation=orientation,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/centrality.degree", G, config, mutate_property)

        return DegreeMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        orientation: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DegreeStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            orientation=orientation,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/centrality.degree", G, config)

        return DegreeStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        orientation: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            orientation=orientation,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/centrality.degree", G, config)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        orientation: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[int] = None,
    ) -> DegreeWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            orientation=orientation,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/centrality.degree", G, config, write_concurrency, concurrency, write_property
        )

        return DegreeWriteResult(**result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        orientation: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            orientation=orientation,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            relationship_weight_property=relationship_weight_property,
        )
        return self._node_property_endpoints.estimate("v2/centrality.degree.estimate", G, config)
