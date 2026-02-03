from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.v2 import GraphBackend, GraphV2
from graphdatascience.procedure_surface.api.catalog.graph_info import GraphInfo, GraphInfoWithDegrees
from graphdatascience.procedure_surface.arrow.catalog.graph_ops_arrow import GraphOpsArrow


def get_graph(name: str, arrow_client: AuthenticatedArrowClient) -> GraphV2:
    backend = ArrowGraphBackend(name, arrow_client)

    return GraphV2(name, backend)


class ArrowGraphBackend(GraphBackend):
    def __init__(self, name: str, arrow_client: AuthenticatedArrowClient) -> None:
        self._name = name
        self._graph_ops = GraphOpsArrow(arrow_client)

    def graph_info(self) -> GraphInfoWithDegrees:
        results = self._graph_ops.list(self._name)

        if not results:
            raise ValueError(f"There is no projected graph named '{self._name}'")

        return results[0]

    def exists(self) -> bool:
        return any(self._graph_ops.list(self._name))

    def drop(self, fail_if_missing: bool = True) -> GraphInfo | None:
        return self._graph_ops.drop(self._name, fail_if_missing)
