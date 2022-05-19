from typing import List, Set

from pandas.core.frame import DataFrame

from .graph_constructor import GraphConstructor
from .query_runner import QueryRunner


class CypherGraphConstructor(GraphConstructor):
    def __init__(
        self,
        query_runner: QueryRunner,
        graph_name: str,
        concurrency: int,
    ):
        self._query_runner = query_runner
        self._concurrency = concurrency
        self._graph_name = graph_name

    def run(self, node_dfs: List[DataFrame], relationship_dfs: List[DataFrame]) -> None:
        if len(node_dfs) > 1:
            raise ValueError("The community edition of GDS supports only a single node dataframe")

        if len(relationship_dfs) > 1:
            raise ValueError("The community edition of GDS supports only a single relationship dataframe")

        query = (
            f"CALL gds.graph.project.cypher(\n"
            f"'{self._graph_name}',\n"
            f"'{self._node_query(node_dfs[0])}',\n"
            f"'{self._relationship_query(relationship_dfs[0])}',\n"
            f"{{readConcurrency: {self._concurrency}, parameters: {{ nodes: $nodes, relationships: $relationships }}}})"
        )

        self._query_runner.run_query(
            query, {"nodes": node_dfs[0].values.tolist(), "relationships": relationship_dfs[0].values.tolist()}
        )

    def _node_query(self, node_df: DataFrame) -> str:
        node_id_index = node_df.columns.get_loc("nodeId")

        label_query = ""
        if "labels" in node_df.keys():
            label_index = node_df.columns.get_loc("labels")
            label_query = f", node[{label_index}] as labels"

        property_query = ""
        property_columns: Set[str] = set(node_df.keys()) - {"nodeId", "labels"}
        if len(property_columns) > 0:
            property_queries = map(lambda col: f", node[{node_df.columns.get_loc(col)}] as {col}", property_columns)
            property_query = "".join(property_queries)

        return "UNWIND $nodes as node " f"RETURN node[{node_id_index}] as id{label_query}{property_query}"

    def _relationship_query(self, rel_df: DataFrame) -> str:
        source_id_index = rel_df.columns.get_loc("sourceNodeId")
        target_id_index = rel_df.columns.get_loc("targetNodeId")

        type_query = ""
        if "relationshipType" in rel_df.keys():
            type_index = rel_df.columns.get_loc("relationshipType")
            type_query = f", relationship[{type_index}] as type"

        property_query = ""
        property_columns: Set[str] = set(rel_df.keys()) - {"sourceNodeId", "targetNodeId", "relationshipType"}
        if len(property_columns) > 0:
            property_queries = map(
                lambda col: f", relationship[{rel_df.columns.get_loc(col)}] as {col}", property_columns
            )
            property_query = "".join(property_queries)

        return (
            "UNWIND $relationships as relationship "
            f"RETURN relationship[{source_id_index}] as source, relationship[{target_id_index}] as target"
            f"{type_query}{property_query}"
        )
