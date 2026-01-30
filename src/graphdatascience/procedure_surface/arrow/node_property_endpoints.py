from typing import Any

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.endpoints_helper_base import EndpointsHelperBase


class NodePropertyEndpointsHelper(EndpointsHelperBase):
    """
    Helper class for Arrow algorithm endpoints that work with node properties.
    Provides common functionality for job execution, mutation, streaming, and writing.
    """

    def run_job_and_mutate(self, endpoint: str, config: dict[str, Any], mutate_property: str) -> dict[str, Any]:
        return self._run_job_and_mutate(endpoint, config, mutate_property=mutate_property)

    def run_job_and_write(
        self,
        endpoint: str,
        G: GraphV2,
        config: dict[str, Any],
        property_overwrites: str | dict[str, str],
        write_concurrency: int | None = None,
        concurrency: int | None = None,
    ) -> dict[str, Any]:
        return self._run_job_and_write(
            endpoint,
            G,
            config,
            property_overwrites=property_overwrites,
            relationship_type_overwrite=None,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )
