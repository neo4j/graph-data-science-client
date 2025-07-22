from typing import Any, List, Optional

from pandas import DataFrame

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.job_client import JobClient
from ...arrow_client.v2.mutation_client import MutationClient
from ...arrow_client.v2.write_back_client import WriteBackClient
from ...graph.graph_object import Graph
from ..api.estimation_result import EstimationResult
from ..api.wcc_endpoints import WccEndpoints, WccMutateResult, WccStatsResult, WccWriteResult
from ..utils.config_converter import ConfigConverter

WCC_ENDPOINT = "v2/community.wcc"


class WccArrowEndpoints(WccEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[WriteBackClient] = None):
        self._arrow_client = arrow_client
        self._write_back_client = write_back_client

    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        threshold: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> WccMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            threshold=threshold,
        )

        job_id = JobClient.run_job_and_wait(self._arrow_client, WCC_ENDPOINT, config)

        mutate_result = MutationClient.mutate_node_property(self._arrow_client, job_id, mutate_property)
        computation_result = JobClient.get_summary(self._arrow_client, job_id)

        computation_result["nodePropertiesWritten"] = mutate_result.node_properties_written
        computation_result["mutateMillis"] = 0

        return WccMutateResult(**computation_result)

    def stats(
        self,
        G: Graph,
        threshold: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> WccStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            threshold=threshold,
        )

        job_id = JobClient.run_job_and_wait(self._arrow_client, WCC_ENDPOINT, config)
        computation_result = JobClient.get_summary(self._arrow_client, job_id)

        return WccStatsResult(**computation_result)

    def stream(
        self,
        G: Graph,
        min_component_size: Optional[int] = None,
        threshold: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            min_component_size=min_component_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            threshold=threshold,
        )

        job_id = JobClient.run_job_and_wait(self._arrow_client, WCC_ENDPOINT, config)
        return JobClient.stream_results(self._arrow_client, G.name(), job_id)

    def write(
        self,
        G: Graph,
        write_property: str,
        min_component_size: Optional[int] = None,
        threshold: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[int] = None,
    ) -> WccWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            min_component_size=min_component_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            threshold=threshold,
        )

        job_id = JobClient.run_job_and_wait(self._arrow_client, WCC_ENDPOINT, config)
        computation_result = JobClient.get_summary(self._arrow_client, job_id)

        if self._write_back_client is None:
            raise Exception("Write back client is not initialized")

        write_millis = self._write_back_client.write(
            G.name(), job_id, write_concurrency if write_concurrency is not None else concurrency
        )

        computation_result["writeMillis"] = write_millis

        return WccWriteResult(**computation_result)

    def estimate(
        self, graph_name: Optional[str] = None, projection_config: Optional[dict[str, Any]] = None
    ) -> EstimationResult:
        pass
