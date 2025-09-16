from typing import Any

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_backend import GraphBackend, GraphInfo
from graphdatascience.procedure_surface.arrow.catalog_arrow_endpoints import CatalogArrowEndpoints


class ArrowGraphBackend(GraphBackend):
    def __init__(self, name: str, arrow_client: AuthenticatedArrowClient) -> None:
        self._name = name
        self._catalog_endpoints = CatalogArrowEndpoints(arrow_client)

    def graph_info(self) -> GraphInfo:
        results = self._catalog_endpoints.list(self._name)

        if not results:
            raise ValueError(f"There is no projected graph named '{self._name}'")

        return GraphInfo(**results[0].to_dict()) # this is a bit hacky. Maybe graph list should also use the graph info class

    def exists(self) -> bool:
        return any(self._catalog_endpoints.list(self._name))

    def drop(self, fail_if_missing: bool = True) -> dict[str, Any]:
        return self._catalog_endpoints.drop(self._name, fail_if_missing).to_dict()