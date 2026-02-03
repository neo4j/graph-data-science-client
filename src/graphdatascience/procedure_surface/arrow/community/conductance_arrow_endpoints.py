from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.conductance_endpoints import ConductanceEndpoints
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.arrow.stream_result_mapper import map_conductance_stream_result
from graphdatascience.procedure_surface.arrow.table_endpoints_helper import TableEndpointsHelper


class ConductanceArrowEndpoints(ConductanceEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        show_progress: bool = True,
    ):
        self._endpoints_helper = TableEndpointsHelper(arrow_client, show_progress=show_progress)

    def stream(
        self,
        G: GraphV2,
        community_property: str,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        config = self._endpoints_helper.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            community_property=community_property,
            relationship_weight_property=relationship_weight_property,
        )

        result = self._endpoints_helper.run_job_and_stream("v2/community.conductance", G, config)
        map_conductance_stream_result(result)

        return result
