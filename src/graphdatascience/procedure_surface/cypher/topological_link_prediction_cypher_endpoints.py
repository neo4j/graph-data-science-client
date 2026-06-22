from __future__ import annotations

from typing import Any

from neo4j.graph import Node

from graphdatascience.error.cypher_warning_handler import filter_id_func_deprecation_warning
from graphdatascience.procedure_surface.api.topological_link_prediction_endpoints import (
    Direction,
    TopologicalLinkPredictionEndpoints,
)
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType


class TopologicalLinkPredictionCypherEndpoints(TopologicalLinkPredictionEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    @filter_id_func_deprecation_warning()
    def adamic_adar(
        self,
        node1: int | Node,
        node2: int | Node,
        relationship_query: str | None = None,
        direction: Direction = Direction.BOTH,
    ) -> float:
        return self._run_similarity_function(
            "gds.linkprediction.adamicAdar", node1, node2, relationship_query, direction
        )

    @filter_id_func_deprecation_warning()
    def common_neighbors(
        self,
        node1: int | Node,
        node2: int | Node,
        relationship_query: str | None = None,
        direction: Direction = Direction.BOTH,
    ) -> float:
        return self._run_similarity_function(
            "gds.linkprediction.commonNeighbors", node1, node2, relationship_query, direction
        )

    @filter_id_func_deprecation_warning()
    def preferential_attachment(
        self,
        node1: int | Node,
        node2: int | Node,
        relationship_query: str | None = None,
        direction: Direction = Direction.BOTH,
    ) -> float:
        return self._run_similarity_function(
            "gds.linkprediction.preferentialAttachment", node1, node2, relationship_query, direction
        )

    @filter_id_func_deprecation_warning()
    def resource_allocation(
        self,
        node1: int | Node,
        node2: int | Node,
        relationship_query: str | None = None,
        direction: Direction = Direction.BOTH,
    ) -> float:
        return self._run_similarity_function(
            "gds.linkprediction.resourceAllocation", node1, node2, relationship_query, direction
        )

    @filter_id_func_deprecation_warning()
    def same_community(
        self,
        node1: int | Node,
        node2: int | Node,
        community_property: str = "community",
    ) -> float:
        query = """
        MATCH (n), (m)
        WHERE id(n) = $node1 AND id(m) = $node2
        RETURN gds.linkprediction.sameCommunity(n, m, $communityProperty) AS score
        """

        node_id1 = node1 if isinstance(node1, int) else node1.id
        node_id2 = node2 if isinstance(node2, int) else node2.id

        params = {"node1": node_id1, "node2": node_id2, "communityProperty": community_property}

        return self._run_query(query, params)

    @filter_id_func_deprecation_warning()
    def total_neighbors(
        self,
        node1: int | Node,
        node2: int | Node,
        relationship_query: str | None = None,
        direction: Direction = Direction.BOTH,
    ) -> float:
        return self._run_similarity_function(
            "gds.linkprediction.totalNeighbors", node1, node2, relationship_query, direction
        )

    def _run_similarity_function(
        self,
        function: str,
        node1: int | Node,
        node2: int | Node,
        relationship_query: str | None,
        direction: Direction,
    ) -> float:
        query = f"""
        MATCH (n), (m)
        WHERE id(n) = $node1 AND id(m) = $node2
        RETURN {function}(n, m, $config) AS score
        """

        config = {
            "direction": direction.value,
            "relationshipQuery": relationship_query,
        }

        node_id1 = node1 if isinstance(node1, int) else node1.id
        node_id2 = node2 if isinstance(node2, int) else node2.id

        params = {"node1": node_id1, "node2": node_id2, "config": config}

        return self._run_query(query, params)

    def _run_query(self, query: str, params: dict[str, Any]) -> float:
        result = self._query_runner.run_cypher(
            query,
            QueryType.USER_TRANSPILED,
            params,
            mode=QueryMode.READ,
        )

        if result.empty:
            raise ValueError("Could not find the specified nodes in the graph")

        return result.squeeze()  # type: ignore[no-any-return]
