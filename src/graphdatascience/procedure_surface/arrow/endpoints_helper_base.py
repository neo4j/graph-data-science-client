from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.data_mapper_utils import deserialize_single
from ...arrow_client.v2.job_client import JobClient
from ...arrow_client.v2.mutation_client import MutationClient
from ...arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from ..api.estimation_result import EstimationResult
from ..utils.config_converter import ConfigConverter


class EndpointsHelperBase:
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = True,
    ):
        self._arrow_client = arrow_client
        self._write_back_client = write_back_client
        self._show_progress = show_progress

    def run_job_and_get_summary(self, endpoint: str, config: dict[str, Any]) -> dict[str, Any]:
        """Run a job and return the computation summary."""
        show_progress: bool = config.get("logProgress", True) and self._show_progress

        job_id = JobClient.run_job_and_wait(self._arrow_client, endpoint, config, show_progress)
        result = JobClient.get_summary(self._arrow_client, job_id)
        if config := result.get("configuration", None):
            self._drop_write_internals(config)
        return result

    def _run_job_and_mutate(
        self,
        endpoint: str,
        config: dict[str, Any],
        *,
        mutate_property: str | None = None,
        mutate_relationship_type: str | None = None,
    ) -> dict[str, Any]:
        """Run a job, mutate node properties, and return summary with mutation result."""
        show_progress = config.get("logProgress", True) and self._show_progress
        job_id = JobClient.run_job_and_wait(self._arrow_client, endpoint, config, show_progress)

        computation_result = JobClient.get_summary(self._arrow_client, job_id)

        if mutate_relationship_type:
            mutate_result = MutationClient.mutate_relationship_property(
                self._arrow_client, job_id, mutate_relationship_type, mutate_property
            )
        elif mutate_property:
            mutate_result = MutationClient.mutate_node_property(self._arrow_client, job_id, mutate_property)
        else:
            raise ValueError("Either mutate_property or mutate_relationship_type must be provided for mutation.")

        computation_result["mutateMillis"] = mutate_result.mutate_millis
        if mutate_property:
            # modify computation result to include mutation details
            computation_result["nodePropertiesWritten"] = mutate_result.node_properties_written
        if mutate_relationship_type:
            computation_result["relationshipsWritten"] = mutate_result.relationships_written

        if (config := computation_result.get("configuration", None)) is not None:
            config["mutateProperty"] = mutate_property
            if mutate_relationship_type is not None:
                config["mutateRelationshipType"] = mutate_relationship_type
            self._drop_write_internals(config)

        return computation_result

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

        if self._write_back_client is None:
            raise Exception("Write back client is not initialized")

        if isinstance(property_overwrites, str):
            # The remote write back procedure allows specifying a single overwrite. The key is ignored.
            property_overwrites = {property_overwrites: property_overwrites}

        write_result = self._write_back_client.write(
            G.name(),
            job_id,
            concurrency=write_concurrency if write_concurrency is not None else concurrency,
            property_overwrites=property_overwrites,
            relationship_type_overwrite=relationship_type_overwrite,
            log_progress=show_progress,
        )

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

    def _drop_write_internals(self, config: dict[str, Any]) -> None:
        config.pop("writeConcurrency", None)
        config.pop("writeToResultStore", None)
        config.pop("writeProperty", None)
        config.pop("writeRelationshipType", None)
        config.pop("writeMillis", None)
