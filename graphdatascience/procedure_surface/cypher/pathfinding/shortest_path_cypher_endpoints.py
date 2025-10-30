from __future__ import annotations

from graphdatascience.procedure_surface.api.pathfinding.shortest_path_endpoints import (
    ShortestPathEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.source_target_astar_endpoints import (
    SourceTargetAStarEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.source_target_dijkstra_endpoints import (
    SourceTargetDijkstraEndpoints,
)
from graphdatascience.procedure_surface.api.pathfinding.source_target_yens_endpoints import (
    SourceTargetYensEndpoints,
)
from graphdatascience.procedure_surface.cypher.pathfinding.source_target_astar_cypher_endpoints import (
    AStarCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pathfinding.source_target_dijkstra_cypher_endpoints import (
    DijkstraCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pathfinding.source_target_yens_cypher_endpoints import (
    YensCypherEndpoints,
)
from graphdatascience.query_runner.query_runner import QueryRunner


class ShortestPathCypherEndpoints(ShortestPathEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner
        self._dijkstra = DijkstraCypherEndpoints(query_runner)
        self._astar = AStarCypherEndpoints(query_runner)
        self._yens = YensCypherEndpoints(query_runner)

    @property
    def dijkstra(self) -> SourceTargetDijkstraEndpoints:
        return self._dijkstra

    @property
    def astar(self) -> SourceTargetAStarEndpoints:
        return self._astar

    @property
    def yens(self) -> SourceTargetYensEndpoints:
        return self._yens
