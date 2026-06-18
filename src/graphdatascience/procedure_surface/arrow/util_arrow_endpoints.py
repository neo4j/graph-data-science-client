from __future__ import annotations

from typing import Any

from neo4j.graph import Node

from graphdatascience.error.cypher_warning_handler import filter_id_func_deprecation_warning
from graphdatascience.error.standalone_session_error import NotAvailableInStandaloneSessions
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.util_endpoints import UtilEndpoints
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType


class UtilArrowEndpoints(UtilEndpoints):
    def __init__(self, db_query_runner: QueryRunner | None):
        self._db_query_runner = db_query_runner

    def _require_db(self) -> QueryRunner:
        if self._db_query_runner is None:
            raise NotAvailableInStandaloneSessions("Util endpoints")
        return self._db_query_runner

    @filter_id_func_deprecation_warning()
    def as_node(self, node_id: int) -> Node:
        return (  # type: ignore[no-any-return]
            self._require_db()
            .run_retryable_cypher(
                "MATCH (n) WHERE id(n) = $nodeId RETURN n",
                QueryType.USER_TRANSPILED,
                {"nodeId": node_id},
                mode=QueryMode.READ,
            )
            .squeeze()
        )

    @filter_id_func_deprecation_warning()
    def as_nodes(self, node_ids: list[int]) -> list[Node]:
        return (  # type: ignore[no-any-return]
            self._require_db()
            .run_retryable_cypher(
                "MATCH (n) WHERE id(n) IN $nodeIds RETURN collect(n)",
                QueryType.USER_TRANSPILED,
                {"nodeIds": node_ids},
                mode=QueryMode.READ,
            )
            .squeeze()
        )

    def node_property(self, G: Graph, node_id: int, property_key: str, node_label: str = "*") -> Any:
        raise NotImplementedError(
            "`node_property` is not available in AuraGDS sessions. "
            "Stream the node properties once and filter on the client side instead, e.g.:\n"
            "    df = gds.graph.node_properties.stream(G, [property_key])\n"
            "    df.loc[df['nodeId'] == node_id, 'propertyValue']"
        )
