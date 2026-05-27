from __future__ import annotations

from collections import OrderedDict
from typing import Any

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.job_client import JobClient
from ...arrow_client.v2.mutation_client import MutationClient


class MutationRunner:
    def __init__(self, arrow_client: AuthenticatedArrowClient):
        self._arrow_client = arrow_client

    def run_mutation(
        self,
        job_id: str,
        *,
        mutate_property: str | None = None,
        mutate_relationship_type: str | None = None,
        mutate_property_overwrites: OrderedDict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Mutate the in-memory graph from a completed job and return the augmented summary."""
        if mutate_relationship_type:
            mutate_result = MutationClient.mutate_relationship_property(
                self._arrow_client, job_id, mutate_relationship_type, mutate_property
            )
        elif mutate_property:
            mutate_result = MutationClient.mutate_node_property(self._arrow_client, job_id, mutate_property)
        elif mutate_property_overwrites:
            mutate_result = MutationClient.mutate_node_properties(
                self._arrow_client, job_id, mutate_property_overwrites
            )
        else:
            raise ValueError(
                "Provide one of: mutate_property, mutate_relationship_type, or mutate_property_overwrites."
            )

        computation_result = JobClient.get_summary(self._arrow_client, job_id)
        computation_result["mutateMillis"] = mutate_result.mutate_millis
        if mutate_property or mutate_property_overwrites:
            computation_result["nodePropertiesWritten"] = mutate_result.node_properties_written
        if mutate_relationship_type:
            computation_result["relationshipsWritten"] = mutate_result.relationships_written

        if (nested_config := computation_result.get("configuration", None)) is not None:
            if mutate_property:
                nested_config["mutateProperty"] = mutate_property
            if mutate_property_overwrites:
                nested_config["mutateProperty"] = next(iter(mutate_property_overwrites.values()))
            if mutate_relationship_type is not None:
                nested_config["mutateRelationshipType"] = mutate_relationship_type
            MutationRunner.drop_write_internals(nested_config)

        return computation_result

    @staticmethod
    def drop_write_internals(config: dict[str, Any]) -> None:
        config.pop("writeConcurrency", None)
        config.pop("writeToResultStore", None)
        config.pop("writeProperty", None)
        config.pop("writeRelationshipType", None)
        config.pop("writeMillis", None)
