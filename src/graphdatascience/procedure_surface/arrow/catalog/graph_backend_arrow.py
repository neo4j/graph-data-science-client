from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph import Graph
from graphdatascience.graph.graph_backend import GraphBackend
from graphdatascience.graph.graph_info import GraphInfo, GraphInfoWithDegrees
from graphdatascience.procedure_surface.arrow.catalog.graph_ops_arrow import GraphOpsArrow
from graphdatascience.session.graph_mapping_context import GraphMappingContext


def get_graph(
    name: str,
    arrow_client: AuthenticatedArrowClient,
    graph_mapping_context: GraphMappingContext | None = None,
) -> Graph:
    backend = ArrowGraphBackend(name, arrow_client, graph_mapping_context)

    return Graph(name, backend)


class ArrowGraphBackend(GraphBackend):
    def __init__(
        self,
        name: str,
        arrow_client: AuthenticatedArrowClient,
        graph_mapping_context: GraphMappingContext | None = None,
    ) -> None:
        self._name = name
        self._graph_ops = GraphOpsArrow(arrow_client, graph_mapping_context)

    def graph_info(self) -> GraphInfoWithDegrees:
        results = self._graph_ops.list(self._name, include_mappings=False)

        if not results:
            raise ValueError(f"There is no projected graph named '{self._name}'")

        return results[0]

    def exists(self) -> bool:
        return any(self._graph_ops.list(self._name, include_mappings=False))

    def drop(self, fail_if_missing: bool = True) -> GraphInfo | None:
        return self._graph_ops.drop(self._name, fail_if_missing)
