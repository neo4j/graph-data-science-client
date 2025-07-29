import json
from typing import Optional, List

from graphdatascience import Graph, QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize_single, deserialize
from graphdatascience.procedure_surface.api.catalog_endpoints import CatalogEndpoints, GraphListResult


class CatalogArrowEndpoints(CatalogEndpoints):

    def __init__(self, arrow_client: AuthenticatedArrowClient, query_runner: Optional[QueryRunner] = None):
        self._arrow_client = arrow_client
        self._query_runner = query_runner


    def list(self, G: Optional[Graph] = None) -> List[GraphListResult]:
        payload = { "graphName": G.name() } if G else {}

        result = self._arrow_client.do_action_with_retry(
            "V2/graph.list",
            json.dumps(payload).encode("utf-8")
        )

        return [GraphListResult(**x) for x in deserialize(result)]

