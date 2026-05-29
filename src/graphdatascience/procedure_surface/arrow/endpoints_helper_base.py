from __future__ import annotations

from collections import OrderedDict
from typing import Any

from pandas import DataFrame

from graphdatascience.graph.v2.graph_api import GraphV2

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.data_mapper_utils import deserialize_single
from ...arrow_client.v2.job_client import JobClient
from ...query_runner.protocol.write_protocols import WriteProtocol
from ...query_runner.termination_flag import TerminationFlag
from ..api.estimation_result import EstimationResult
from ..api.job_handle import JobHandle
from ..api.write_job_handle import WriteJobHandle
from ..utils.config_converter import ConfigConverter
from .mutation_runner import MutationRunner


class EndpointsHelperBase:
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = True,
    ):
        self._arrow_client = arrow_client
        self._write_protocol = write_protocol
        self._show_progress = show_progress
        self._mutation_runner = MutationRunner(arrow_client)

    def run_job(self, G: GraphV2, endpoint: str, config: dict[str, Any]) -> JobHandle:
        """Starts a job and returns associated JobHandle."""
        show_progress: bool = config.get("logProgress", True) and self._show_progress

        job_id = JobClient.run_job(self._arrow_client, endpoint, config)
        return JobHandle(self._arrow_client, self._write_protocol, job_id, G, show_progress, endpoint=endpoint)

    def run_job_and_get_summary(self, endpoint: str, config: dict[str, Any]) -> dict[str, Any]:
        """Run a job and return the computation summary."""
        show_progress: bool = config.get("logProgress", True) and self._show_progress

        job_id = JobClient.run_job_and_wait(self._arrow_client, endpoint, config, show_progress)
        result = JobClient.get_summary(self._arrow_client, job_id)
        if nested_config := result.get("configuration", None):
            MutationRunner.drop_write_internals(nested_config)
        return result

    def _run_job_and_mutate(
        self,
        endpoint: str,
        config: dict[str, Any],
        *,
        mutate_property: str | None = None,
        mutate_property_overwrites: OrderedDict[str, str] | None = None,
        mutate_relationship_type: str | None = None,
    ) -> dict[str, Any]:
        """Run a job, mutate node properties, and return summary with mutation result."""
        show_progress = config.get("logProgress", True) and self._show_progress
        job_id = JobClient.run_job_and_wait(self._arrow_client, endpoint, config, show_progress)
        return self._mutation_runner.run_mutation(
            job_id,
            mutate_property=mutate_property,
            mutate_relationship_type=mutate_relationship_type,
            mutate_property_overwrites=mutate_property_overwrites,
        )

    def run_job_and_stream(self, endpoint: str, G: GraphV2, config: dict[str, Any]) -> DataFrame:
        """Run a job and return streamed results."""
        show_progress = config.get("logProgress", True) and self._show_progress
        job_id = JobClient.run_job_and_wait(self._arrow_client, endpoint, config, show_progress=show_progress)
        return JobClient.stream_results(self._arrow_client, G.name(), job_id)

    def _run_job_and_write(
        self,
        endpoint: str,
        G: GraphV2,
        config: dict[str, Any],
        *,
        relationship_type_overwrite: str | None = None,
        property_overwrites: str | dict[str, str] | None = None,
        write_concurrency: int | None,
        concurrency: int | None,
    ) -> dict[str, Any]:
        """Run a job, write results, and return summary with write time."""
        show_progress = config.get("logProgress", True) and self._show_progress
        job_id = JobClient.run_job_and_wait(self._arrow_client, endpoint, config, show_progress=show_progress)
        computation_result = JobClient.get_summary(self._arrow_client, job_id)

        if self._write_protocol is None:
            raise Exception("Write back is not supported by this session.")

        job_handle = WriteJobHandle.create(
            self._write_protocol,
            G.name(),
            job_id,
            TerminationFlag.create(),
            concurrency=write_concurrency if write_concurrency is not None else concurrency,
            property_overwrites=property_overwrites,
            relationship_type_overwrite=relationship_type_overwrite,
            log_progress=show_progress,
        )

        write_result = job_handle.result(wait=True)

        # modify computation result to include write details
        computation_result["writeMillis"] = write_result.write_millis

        if relationship_type_overwrite:
            computation_result["relationshipsWritten"] = write_result.written_relationships
        if property_overwrites:
            computation_result["propertiesWritten"] = write_result.written_node_properties

        return computation_result

    def create_base_config(self, G: GraphV2, **kwargs: Any) -> dict[str, Any]:
        """Create base configuration with common parameters."""
        return ConfigConverter.convert_to_gds_config(graph_name=G.name(), **kwargs)

    def create_estimate_config(self, **kwargs: Any) -> dict[str, Any]:
        """Create configuration for estimation."""
        return ConfigConverter.convert_to_gds_config(**kwargs)

    def estimate(
        self,
        estimate_endpoint: str,
        G: GraphV2 | dict[str, Any],
        algo_config: dict[str, Any] | None = None,
    ) -> EstimationResult:
        """Estimate memory requirements for the algorithm."""
        if isinstance(G, GraphV2):
            payload = {"graphName": G.name()}
        elif isinstance(G, dict):
            payload = G
        else:
            raise ValueError("Either graph_name or projection_config must be provided.")

        payload.update(algo_config or {})

        res = self._arrow_client.do_action_with_retry(estimate_endpoint, payload)

        return EstimationResult(**deserialize_single(res))
