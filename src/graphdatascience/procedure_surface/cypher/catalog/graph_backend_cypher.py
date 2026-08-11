from __future__ import annotations

from graphdatascience.graph import Graph
from graphdatascience.graph.graph_backend import GraphBackend
from graphdatascience.graph.graph_info import GraphInfo, GraphInfoWithDegrees
from graphdatascience.procedure_surface.cypher.catalog.graph_ops_cypher import GraphOpsCypher
from graphdatascience.query_runner.query_runner import QueryRunner


def get_graph(name: str, query_runner: QueryRunner) -> Graph:
    backend = CypherGraphBackend(name, query_runner)

    return Graph(name, backend)


class CypherGraphBackend(GraphBackend):
    def __init__(self, name: str, query_runner: QueryRunner) -> None:
        self._name = name
        self._graph_ops = GraphOpsCypher(query_runner)
        self._db = query_runner.database()

    def graph_info(self) -> GraphInfoWithDegrees:
        info = self._graph_ops.list(self._name)

        if len(info) == 0:
            raise ValueError(f"There is no projected graph named '{self._name}'")
        if len(info) > 1:
            # for multiple dbs we can have the same graph name. But db + graph name is unique
            info = [g for g in info if g.database == self._db]

        return info[0]

    def exists(self) -> bool:
        return self._graph_ops.exists(self._name)

    def drop(self, fail_if_missing: bool = True) -> GraphInfo | None:
        return self._graph_ops.drop(self._name, fail_if_missing)
