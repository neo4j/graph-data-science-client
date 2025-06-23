from typing import Any, List, Optional

from pandas import DataFrame

from ...arrow_client.authenticated_arrow_client import AuthenticatedArrowClient
from ...arrow_client.v2.job_client import JobClient
from ...arrow_client.v2.mutation_client import MutationClient
from ...arrow_client.v2.write_back_client import WriteBackClient
from ...graph.graph_object import Graph
from ..api.wcc_endpoints import WccEndpoints, WccMutateResult, WccStatsResult, WccWriteResult

WCC_ENDPOINT = "v2/community.wcc"


class WccArrowEndpoints(WccEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[WriteBackClient]):
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
        config = self._build_configuration(
            G,
            concurrency,
            consecutive_ids,
            job_id,
            log_progress,
            None,
            node_labels,
            relationship_types,
            relationship_weight_property,
            seed_property,
            sudo,
            threshold,
        )

        job_id = JobClient.run_job_and_wait(self._arrow_client, WCC_ENDPOINT, config)

        mutate_result = MutationClient.mutate_node_property(self._arrow_client, job_id, mutate_property)
        computation_result = JobClient.get_summary(self._arrow_client, job_id)

        return WccMutateResult(
            computation_result["componentCount"],
            computation_result["componentDistribution"],
            computation_result["preProcessingMillis"],
            computation_result["computeMillis"],
            computation_result["postProcessingMillis"],
            0,
            mutate_result.nodePropertiesWritten,
            computation_result["configuration"],
        )

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
        config = self._build_configuration(
            G,
            concurrency,
            consecutive_ids,
            job_id,
            log_progress,
            None,
            node_labels,
            relationship_types,
            relationship_weight_property,
            seed_property,
            sudo,
            threshold,
        )

        job_id = JobClient.run_job_and_wait(self._arrow_client, WCC_ENDPOINT, config)
        computation_result = JobClient.get_summary(self._arrow_client, job_id)

        return WccStatsResult(
            computation_result["componentCount"],
            computation_result["componentDistribution"],
            computation_result["preProcessingMillis"],
            computation_result["computeMillis"],
            computation_result["postProcessingMillis"],
            computation_result["configuration"],
        )

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
        config = self._build_configuration(
            G,
            concurrency,
            consecutive_ids,
            job_id,
            log_progress,
            min_component_size,
            node_labels,
            relationship_types,
            relationship_weight_property,
            seed_property,
            sudo,
            threshold,
        )

        job_id = JobClient.run_job_and_wait(self._arrow_client, WCC_ENDPOINT, config)
        return JobClient.stream_results(self._arrow_client, job_id)

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
        config = self._build_configuration(
            G,
            concurrency,
            consecutive_ids,
            job_id,
            log_progress,
            min_component_size,
            node_labels,
            relationship_types,
            relationship_weight_property,
            seed_property,
            sudo,
            threshold,
        )

        job_id = JobClient.run_job_and_wait(self._arrow_client, WCC_ENDPOINT, config)
        computation_result = JobClient.get_summary(self._arrow_client, job_id)

        write_millis = self._write_back_client.write(
            G.name(), job_id, write_concurrency if write_concurrency is not None else concurrency
        )

        return WccWriteResult(
            computation_result["componentCount"],
            computation_result["componentDistribution"],
            computation_result["preProcessingMillis"],
            computation_result["computeMillis"],
            write_millis,
            computation_result["postProcessingMillis"],
            computation_result["nodePropertiesWritten"],
            computation_result["configuration"],
        )

    @staticmethod
    def _build_configuration(
        G: Graph,
        concurrency: Optional[int],
        consecutive_ids: Optional[bool],
        job_id: Optional[str],
        log_progress: Optional[bool],
        min_component_size: Optional[int],
        node_labels: Optional[List[str]],
        relationship_types: Optional[List[str]],
        relationship_weight_property: Optional[str],
        seed_property: Optional[str],
        sudo: Optional[bool],
        threshold: Optional[float],
    ):
        config: dict[str, Any] = {
            "graphName": G.name(),
        }

        if min_component_size is not None:
            config["minComponentSize"] = min_component_size
        if threshold is not None:
            config["threshold"] = threshold
        if relationship_types is not None:
            config["relationshipTypes"] = relationship_types
        if node_labels is not None:
            config["nodeLabels"] = node_labels
        if sudo is not None:
            config["sudo"] = sudo
        if log_progress is not None:
            config["logProgress"] = log_progress
        if concurrency is not None:
            config["concurrency"] = concurrency
        if job_id is not None:
            config["jobId"] = job_id
        if seed_property is not None:
            config["seedProperty"] = seed_property
        if consecutive_ids is not None:
            config["consecutiveIds"] = consecutive_ids
        if relationship_weight_property is not None:
            config["relationshipWeightProperty"] = relationship_weight_property

        return config
