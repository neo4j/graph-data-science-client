from typing import Optional

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.catalog.graph_api import Graph
from graphdatascience.procedure_surface.api.catalog.graph_backend import GraphBackend
from graphdatascience.procedure_surface.api.catalog.graph_info import GraphInfo
from graphdatascience.procedure_surface.arrow.catalog.graph_ops_arrow import GraphOpsArrow


def wrap_graph(name: str, arrow_client: AuthenticatedArrowClient) -> Graph:
    backend = ArrowGraphBackend(name, arrow_client)

    return Graph(name, backend)


class ArrowGraphBackend(GraphBackend):
    def __init__(self, name: str, arrow_client: AuthenticatedArrowClient) -> None:
        self._name = name
        self._catalog_endpoints = GraphOpsArrow(arrow_client)

    def graph_info(self) -> GraphInfo:
        results = self._catalog_endpoints.list(self._name)

        if not results:
            raise ValueError(f"There is no projected graph named '{self._name}'")

        return results[0]

    def exists(self) -> bool:
        return any(self._catalog_endpoints.list(self._name))

    def drop(self, fail_if_missing: bool = True) -> Optional[GraphInfo]:
        return self._catalog_endpoints.drop(self._name, fail_if_missing)
