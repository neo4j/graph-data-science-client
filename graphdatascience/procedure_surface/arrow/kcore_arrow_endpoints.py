import json
from typing import Any, List, Optional

from pandas import DataFrame

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.data_mapper_utils import deserialize_single
from ...arrow_client.v2.job_client import JobClient
from ...arrow_client.v2.mutation_client import MutationClient
from ...arrow_client.v2.write_back_client import WriteBackClient
from ...graph.graph_object import Graph
from ..api.estimation_result import EstimationResult
from ..api.kcore_endpoints import KCoreEndpoints, KCoreMutateResult, KCoreStatsResult, KCoreWriteResult
from ..utils.config_converter import ConfigConverter

KCORE_ENDPOINT = "v2/community.kcore"


class KCoreArrowEndpoints(KCoreEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[WriteBackClient] = None):
        self._arrow_client = arrow_client
        self._write_back_client = write_back_client

    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        target_nodes: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> KCoreMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
            target_nodes=target_nodes,
        )

        job_id = JobClient.run_job_and_wait(self._arrow_client, KCORE_ENDPOINT, config)

        mutate_result = MutationClient.mutate_node_property(self._arrow_client, job_id, mutate_property)
        computation_result = JobClient.get_summary(self._arrow_client, job_id)

        computation_result["nodePropertiesWritten"] = mutate_result.node_properties_written
        computation_result["mutateMillis"] = 0

        return KCoreMutateResult(**computation_result)

    def stats(
        self,
        G: Graph,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        target_nodes: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> KCoreStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
            target_nodes=target_nodes,
        )

        job_id = JobClient.run_job_and_wait(self._arrow_client, KCORE_ENDPOINT, config)
        computation_result = JobClient.get_summary(self._arrow_client, job_id)

        return KCoreStatsResult(**computation_result)

    def stream(
        self,
        G: Graph,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        target_nodes: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
            target_nodes=target_nodes,
        )

        job_id = JobClient.run_job_and_wait(self._arrow_client, KCORE_ENDPOINT, config)
        return JobClient.stream_results(self._arrow_client, G.name(), job_id)

    def write(
        self,
        G: Graph,
        write_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        target_nodes: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[int] = None,
    ) -> KCoreWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            graph_name=G.name(),
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
            target_nodes=target_nodes,
        )

        job_id = JobClient.run_job_and_wait(self._arrow_client, KCORE_ENDPOINT, config)
        computation_result = JobClient.get_summary(self._arrow_client, job_id)

        if self._write_back_client is None:
            raise Exception("Write back client is not initialized")

        write_millis = self._write_back_client.write(
            G.name(), job_id, write_concurrency if write_concurrency is not None else concurrency
        )

        computation_result["writeMillis"] = write_millis

        return KCoreWriteResult(**computation_result)

    def estimate(
        self, G: Optional[Graph] = None, projection_config: Optional[dict[str, Any]] = None
    ) -> EstimationResult:
        if G is not None:
            payload = {"graphName": G.name()}
        elif projection_config is not None:
            payload = projection_config
        else:
            raise ValueError("Either graph_name or projection_config must be provided.")

        res = self._arrow_client.do_action_with_retry(
            "v2/community.kcore.estimate", json.dumps(payload).encode("utf-8")
        )

        return EstimationResult(**deserialize_single(res))
