from typing import Any, Dict, Optional, Union

from pandas import DataFrame

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.data_mapper_utils import deserialize_single
from ...arrow_client.v2.job_client import JobClient
from ...arrow_client.v2.mutation_client import MutationClient
from ...arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from ...graph.graph_object import Graph
from ..api.estimation_result import EstimationResult
from ..utils.config_converter import ConfigConverter


class NodePropertyEndpoints:
    """
    Helper class for Arrow algorithm endpoints that work with node properties.
    Provides common functionality for job execution, mutation, streaming, and writing.
    """

    def __init__(
        self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[RemoteWriteBackClient] = None
    ):
        self._arrow_client = arrow_client
        self._write_back_client = write_back_client

    def run_job_and_get_summary(self, endpoint: str, G: Graph, config: Dict[str, Any]) -> Dict[str, Any]:
        """Run a job and return the computation summary."""
        job_id = JobClient.run_job_and_wait(self._arrow_client, endpoint, config)
        return JobClient.get_summary(self._arrow_client, job_id)

    def run_job_and_mutate(
        self, endpoint: str, G: Graph, config: Dict[str, Any], mutate_property: str
    ) -> Dict[str, Any]:
        """Run a job, mutate node properties, and return summary with mutation result."""
        job_id = JobClient.run_job_and_wait(self._arrow_client, endpoint, config)
        mutate_result = MutationClient.mutate_node_property(self._arrow_client, job_id, mutate_property)
        computation_result = JobClient.get_summary(self._arrow_client, job_id)

        # modify computation result to include mutation details
        computation_result["nodePropertiesWritten"] = mutate_result.node_properties_written
        computation_result["mutateMillis"] = mutate_result.mutate_millis

        if (config := computation_result.get("configuration", None)) is not None:
            config["mutateProperty"] = mutate_property
            config.pop("writeConcurrency", None)
            config.pop("writeToResultStore", None)
            config.pop("writeProperty", None)
            config.pop("writeMillis", None)

        return computation_result

    def run_job_and_stream(self, endpoint: str, G: Graph, config: Dict[str, Any]) -> DataFrame:
        """Run a job and return streamed results."""
        job_id = JobClient.run_job_and_wait(self._arrow_client, endpoint, config)
        return JobClient.stream_results(self._arrow_client, G.name(), job_id)

    def run_job_and_write(
        self,
        endpoint: str,
        G: Graph,
        config: Dict[str, Any],
        write_concurrency: Optional[int] = None,
        concurrency: Optional[int] = None,
        property_overwrites: Optional[dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Run a job, write results, and return summary with write time."""
        job_id = JobClient.run_job_and_wait(self._arrow_client, endpoint, config)
        computation_result = JobClient.get_summary(self._arrow_client, job_id)

        if self._write_back_client is None:
            raise Exception("Write back client is not initialized")

        write_result = self._write_back_client.write(
            G.name(),
            job_id,
            concurrency=write_concurrency if write_concurrency is not None else concurrency,
            property_overwrites=property_overwrites,
        )

        # modify computation result to include write details
        computation_result["writeMillis"] = write_result.write_millis

        return computation_result

    def create_base_config(self, G: Graph, **kwargs: Any) -> Dict[str, Any]:
        """Create base configuration with common parameters."""
        return ConfigConverter.convert_to_gds_config(graph_name=G.name(), **kwargs)

    def create_estimate_config(self, **kwargs: Any) -> Dict[str, Any]:
        """Create configuration for estimation."""
        return ConfigConverter.convert_to_gds_config(**kwargs)

    def estimate(
        self,
        estimate_endpoint: str,
        G: Union[Graph, dict[str, Any]],
        algo_config: Optional[dict[str, Any]] = None,
    ) -> EstimationResult:
        """Estimate memory requirements for the algorithm."""
        if isinstance(G, Graph):
            payload = {"graphName": G.name()}
        elif isinstance(G, dict):
            payload = G
        else:
            raise ValueError("Either graph_name or projection_config must be provided.")

        payload.update(algo_config or {})

        res = self._arrow_client.do_action_with_retry(estimate_endpoint, payload)

        return EstimationResult(**deserialize_single(res))
