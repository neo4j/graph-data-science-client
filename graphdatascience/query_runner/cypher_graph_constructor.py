import warnings
from typing import Any, List, Set, Tuple

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
        if self._should_warn_about_arrow_missing():
            warnings.warn(
                "GDS Enterprise users can use Apache Arrow for fast graph construction; please see the documentation "
                "for instructions on how to enable it. Without Arrow enabled, this installation will use community "
                "edition graph construction (slower)"
            )

        if len(node_dfs) > 1:
            raise ValueError("The GDS Community edition graph construction supports only a single node dataframe")

        if len(relationship_dfs) > 1:
            raise ValueError("The GDS Community edition graph construction supports at most one relationship dataframe")

        query = (
            "CALL gds.graph.project.cypher("
            "$graph_name, "
            "$node_query, "
            "$relationship_query, "
            "{readConcurrency: $read_concurrency, parameters: { nodes: $nodes, relationships: $relationships }})"
        )

        node_query, nodes = self._node_query(node_dfs[0])
        relationship_query, relationships = self._relationship_query(relationship_dfs[0])

        self._query_runner.run_query(
            query,
            {
                "graph_name": self._graph_name,
                "node_query": node_query,
                "relationship_query": relationship_query,
                "read_concurrency": self._concurrency,
                "nodes": nodes,
                "relationships": relationships,
            },
        )

    def _should_warn_about_arrow_missing(self) -> bool:
        try:
            license: str = self._query_runner.run_query(
                "CALL gds.debug.sysInfo() YIELD key, value WHERE key = 'gdsEdition' RETURN value"
            ).squeeze()
            should_warn = license == "Licensed"
        except Exception as e:
            # It's not a user's concern whether Arrow is set up or not in AuraDS.
            if (
                "There is no procedure with the name `gds.debug.sysInfo` "
                "registered for this database instance." in str(e)
            ):
                should_warn = False
            else:
                raise e

        return should_warn

    def _node_query(self, node_df: DataFrame) -> Tuple[str, List[List[Any]]]:
        node_list = node_df.values.tolist()
        node_id_index = node_df.columns.get_loc("nodeId")

        label_query = ""
        if "labels" in node_df.keys():
            label_index = node_df.columns.get_loc("labels")
            label_query = f", node[{label_index}] as labels"

            # Make sure every node has a list of labels
            for node in node_list:
                labels = node[label_index]
                if isinstance(labels, List):
                    continue
                node[label_index] = [labels]

        property_query = ""
        property_columns: Set[str] = set(node_df.keys()) - {"nodeId", "labels"}
        if len(property_columns) > 0:
            property_queries = (f", node[{node_df.columns.get_loc(col)}] as {col}" for col in property_columns)
            property_query = "".join(property_queries)

        return f"UNWIND $nodes as node RETURN node[{node_id_index}] as id{label_query}{property_query}", node_list

    def _relationship_query(self, rel_df: DataFrame) -> Tuple[str, List[List[Any]]]:
        rel_list = rel_df.values.tolist()
        source_id_index = rel_df.columns.get_loc("sourceNodeId")
        target_id_index = rel_df.columns.get_loc("targetNodeId")

        type_query = ""
        if "relationshipType" in rel_df.keys():
            type_index = rel_df.columns.get_loc("relationshipType")
            type_query = f", relationship[{type_index}] as type"

        property_query = ""
        property_columns: Set[str] = set(rel_df.keys()) - {"sourceNodeId", "targetNodeId", "relationshipType"}
        if len(property_columns) > 0:
            property_queries = (f", relationship[{rel_df.columns.get_loc(col)}] as {col}" for col in property_columns)
            property_query = "".join(property_queries)

        return (
            "UNWIND $relationships as relationship "
            f"RETURN relationship[{source_id_index}] as source, relationship[{target_id_index}] as target"
            f"{type_query}{property_query}",
            rel_list,
        )
