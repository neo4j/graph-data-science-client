from typing import Any, Dict

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.endpoints_helper_base import EndpointsHelperBase


# TODO find common parts with node_property_endpoints and refactor into a base class
class RelationshipEndpointsHelper(EndpointsHelperBase):
    """
    Helper class for Arrow algorithm endpoints that work with relationships.
    Provides common functionality for job execution, mutation, streaming, and writing.
    """

    def run_job_and_mutate(
        self, endpoint: str, config: Dict[str, Any], mutate_property: str, mutate_relationship_type: str
    ) -> Dict[str, Any]:
        """Run a job, mutate node properties, and return summary with mutation result."""
        return self._run_job_and_mutate(
            endpoint,
            config,
            mutate_property=mutate_property,
            mutate_relationship_type=mutate_relationship_type,
        )

    def run_job_and_write(
        self,
        endpoint: str,
        G: GraphV2,
        config: dict[str, Any],
        *,
        relationship_type_overwrite: str,
        property_overwrites: str | dict[str, str] | None = None,
        write_concurrency: int | None,
        concurrency: int | None,
    ) -> dict[str, Any]:
        return self._run_job_and_write(
            endpoint,
            G,
            config,
            relationship_type_overwrite=relationship_type_overwrite,
            property_overwrites=property_overwrites,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )
