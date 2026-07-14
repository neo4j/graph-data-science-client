from __future__ import annotations

from graphdatascience.procedure_surface.api.pathfinding.dag_endpoints import DagEndpoints
from graphdatascience.procedure_surface.api.pathfinding.longest_path_endpoints import LongestPathEndpoints
from graphdatascience.procedure_surface.api.pathfinding.topological_sort_endpoints import TopologicalSortEndpoints
from graphdatascience.procedure_surface.cypher.pathfinding.longest_path_cypher_endpoints import (
    LongestPathCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pathfinding.topological_sort_cypher_endpoints import (
    TopologicalSortCypherEndpoints,
)
from graphdatascience.query_runner.query_runner import QueryRunner


class DagCypherEndpoints(DagEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner
        self._longest_path = LongestPathCypherEndpoints(query_runner)
        self._topological_sort = TopologicalSortCypherEndpoints(query_runner)

    @property
    def longest_path(self) -> LongestPathEndpoints:
        return self._longest_path

    @property
    def topological_sort(self) -> TopologicalSortEndpoints:
        return self._topological_sort
