from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from ..api.estimation_result import EstimationResult
from ..api.scc_endpoints import SccEndpoints, SccMutateResult, SccStatsResult, SccWriteResult
from .node_property_endpoints import NodePropertyEndpoints


class SccArrowEndpoints(SccEndpoints):
    def __init__(
        self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[RemoteWriteBackClient] = None
    ):
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client, write_back_client)

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
    ) -> SccMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/community.scc", G, config, mutate_property)

        return SccMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
    ) -> SccStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/community.scc", G, config)

        return SccStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/community.scc", G, config)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        write_concurrency: Optional[int] = None,
    ) -> SccWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/community.scc", G, config, write_concurrency, concurrency
        )

        return SccWriteResult(**result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        consecutive_ids: Optional[bool] = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
        )
        return self._node_property_endpoints.estimate("v2/community.scc.estimate", G, config)
