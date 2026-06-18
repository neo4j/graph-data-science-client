from __future__ import annotations

from typing import Any

from neo4j.graph import Node

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.util_endpoints import UtilEndpoints
from graphdatascience.query_runner.query_runner import QueryRunner


class UtilCypherEndpoints(UtilEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def as_node(self, node_id: int) -> Node:
        return self._query_runner.call_function(  # type: ignore[no-any-return]
            endpoint="gds.util.asNode",
            params=CallParameters(nodeId=node_id),
        )

    def as_nodes(self, node_ids: list[int]) -> list[Node]:
        return self._query_runner.call_function(  # type: ignore[no-any-return]
            endpoint="gds.util.asNodes",
            params=CallParameters(nodeIds=node_ids),
        )

    def node_property(self, G: Graph, node_id: int, property_key: str, node_label: str = "*") -> Any:
        return self._query_runner.call_function(
            endpoint="gds.util.nodeProperty",
            params=CallParameters(
                graph_name=G.name(),
                node_id=node_id,
                property_key=property_key,
                node_label=node_label,
            ),
        )
