import os
import warnings
from dataclasses import dataclass
from typing import Any, Dict, List, Set, Tuple
from uuid import uuid4

from pandas import DataFrame, concat

from .graph_constructor import GraphConstructor
from .query_runner import QueryRunner
from graphdatascience.server_version.server_version import ServerVersion


@dataclass
class EntityColumnSchema:
    all: Set[str]
    properties: Set[str]


@dataclass
class GraphColumnSchema:
    nodes: EntityColumnSchema
    relationships: EntityColumnSchema


class CypherGraphConstructor(GraphConstructor):
    def __init__(self, query_runner: QueryRunner, graph_name: str, concurrency: int, server_version: ServerVersion):
        self._query_runner = query_runner
        self._concurrency = concurrency
        self._graph_name = graph_name
        self._server_version = server_version

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

        node_df = node_dfs[0]
        rel_df = relationship_dfs[0]

        # Cypher aggregation supports concurrency since 2.3.0
        if self._server_version >= ServerVersion(2, 3, 0):
            self.CypherAggregationRunner(self._query_runner, self._graph_name, self._concurrency).run(node_df, rel_df)
        else:
            self.CyperProjectionRunner(self._query_runner, self._graph_name, self._concurrency).run(node_df, rel_df)

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

    class CypherAggregationRunner:

        _BIT_COL_SUFFIX = "_is_present" + str(uuid4())

        def __init__(self, query_runner: QueryRunner, graph_name: str, concurrency: int):
            self._query_runner = query_runner
            self._concurrency = concurrency
            self._graph_name = graph_name

        def run(self, node_df: DataFrame, relationship_df: DataFrame) -> None:
            graph_schema = self.schema(node_df, relationship_df)

            same_cols = graph_schema.relationships.all.intersection(graph_schema.nodes.all)

            if same_cols:
                raise ValueError(
                    "Expected disjoint column names in node and relationship df "
                    f"but the columns {same_cols} exist in both dfs. Please rename the column in one df."
                )

            aligned_node_df = self.adjust_node_df(node_df, relationship_df, graph_schema)
            aligned_rel_df = self.adjust_rel_df(relationship_df, node_df, graph_schema)

            # concat instead of join as we want to first have all nodes and then the rels
            # this way we dont duplicate the node property data and its cheaper
            combined_df: DataFrame = concat([aligned_node_df, aligned_rel_df], ignore_index=True, copy=False)
            # make column order deterministic
            combined_df = combined_df[sorted(combined_df)]

            # using a List and not a Set to preserve the order
            combined_cols: List[str] = list(combined_df.columns)

            nodes_config = self.nodes_config(combined_cols, graph_schema.nodes.all)
            rels_config = self.rels_config(combined_cols, graph_schema.relationships.all)

            property_clauses: List[str] = []

            all_prop_cols = sorted(list(graph_schema.relationships.properties) + list(graph_schema.nodes.properties))

            for prop_col in all_prop_cols:
                property_clauses.append(self.check_value_clause(combined_cols, prop_col))

            target_id_clause = self.check_value_clause(combined_cols, "targetNodeId")

            property_clauses_str = f", {os.linesep}" if len(property_clauses) > 0 else ""
            property_clauses_str += f", {os.linesep}".join(property_clauses)

            query = (
                "UNWIND $data AS data"
                f" WITH data, {target_id_clause}{property_clauses_str}"
                " RETURN gds.alpha.graph.project("
                f"$graph_name, data[{combined_cols.index('sourceNodeId')}], targetNodeId, "
                f"{nodes_config}, {rels_config}, $configuration)"
            )

            # TODO add orientation here once its supported in 2.3
            configuration = {"readConcurrency": self._concurrency}

            self._query_runner.run_query(
                query,
                {
                    "data": combined_df.values.tolist(),
                    "graph_name": self._graph_name,
                    "configuration": configuration,
                },
            )

        def check_value_clause(self, combined_cols: List[str], col: str) -> str:
            return (
                f"CASE data[{combined_cols.index(col + self._BIT_COL_SUFFIX)}]"
                f" WHEN true THEN data[{combined_cols.index(col)}] ELSE null END AS {col}"
            )

        def schema(self, node_df: DataFrame, rel_df: DataFrame) -> GraphColumnSchema:
            rel_cols = set(rel_df.columns)
            node_cols = set(node_df.columns)

            node_schema = EntityColumnSchema(node_cols, node_cols - {"nodeId", "labels"})

            rel_schema = EntityColumnSchema(rel_cols, rel_cols - {"sourceNodeId", "targetNodeId", "relationshipType"})

            return GraphColumnSchema(node_schema, rel_schema)

        def adjust_node_df(self, node_df: DataFrame, rel_df: DataFrame, schema: GraphColumnSchema) -> DataFrame:
            node_dict: Dict[str, Any] = {
                "sourceNodeId": node_df["nodeId"],
                f"targetNodeId{self._BIT_COL_SUFFIX}": False,
                "targetNodeId": -1,
                "relationshipType": None,
            }

            if "labels" in schema.nodes.all:
                node_dict["sourceNodeLabels"] = node_df["labels"]

            # as we first list all nodes at the top of the df, we dont need to lookup properties for the target node
            for col in schema.nodes.properties:
                node_dict[col + self._BIT_COL_SUFFIX] = True
                node_dict[col] = node_df[col]

            for col in schema.relationships.properties:
                node_dict[col + self._BIT_COL_SUFFIX] = False
                node_dict[col] = rel_df[col][0]  # taking some dummy value which will be ignored in cypher

            return DataFrame(node_dict)

        def adjust_rel_df(self, rel_df: DataFrame, node_df: DataFrame, schema: GraphColumnSchema) -> DataFrame:
            rel_dict = {
                "sourceNodeId": rel_df["sourceNodeId"],
                "targetNodeId": rel_df["targetNodeId"],
                f"targetNodeId{self._BIT_COL_SUFFIX}": True,
            }

            if "relationshipType" in schema.relationships.all:
                rel_dict["relationshipType"] = rel_df["relationshipType"]

            if "labels" in schema.nodes.all:
                rel_dict["sourceNodeLabels"] = None

            for col in schema.relationships.properties:
                rel_dict[col + self._BIT_COL_SUFFIX] = True
                rel_dict[col] = rel_df[col]

            for col in schema.nodes.properties:
                rel_dict[col + self._BIT_COL_SUFFIX] = False
                rel_dict[col] = node_df[col][0]  # taking some dummy value which will be ignored in cypher

            return DataFrame(rel_dict)

        def nodes_config(self, combined_cols: List[str], node_cols: Set[str]) -> str:
            # Cannot use a dictionary as we need to refer to the `data` variable in the cypher query.
            # Otherwise we would just pass a string such as `data[0]`
            nodes_config_fields: List[str] = []

            if "labels" in node_cols:
                nodes_config_fields.append(f"sourceNodeLabels: data[{combined_cols.index('sourceNodeLabels')}]")

            property_columns: List[str] = list(set(node_cols) - {"nodeId", "labels"})
            property_columns.sort()

            # as we first list all nodes at the top of the df, we dont need to lookup properties for the target node
            if len(property_columns) > 0:
                node_properties_config = [f"{col}: {col}" for col in property_columns]
                nodes_config_fields.append(f"sourceNodeProperties: {{{', '.join(node_properties_config)}}}")

            return f"{{{', '.join(nodes_config_fields)}}}"

        def rels_config(self, combined_cols: List[str], rel_cols: Set[str]) -> str:
            rels_config_fields: List[str] = []

            if "relationshipType" in rel_cols:
                rels_config_fields.append(f"relationshipType: data[{combined_cols.index('relationshipType')}]")

            property_columns: List[str] = list(rel_cols - {"sourceNodeId", "targetNodeId", "relationshipType"})
            property_columns.sort()

            if len(property_columns) > 0:
                rel_properties_config = [f"{col}: {col}" for col in property_columns]
                rels_config_fields.append(f"properties: {{{', '.join(rel_properties_config)}}}")

            return f"{{{', '.join(rels_config_fields)}}}"

    class CyperProjectionRunner:
        def __init__(self, query_runner: QueryRunner, graph_name: str, concurrency: int):
            self._query_runner = query_runner
            self._concurrency = concurrency
            self._graph_name = graph_name

        def run(self, node_df: DataFrame, relationship_df: DataFrame) -> None:
            query = (
                "CALL gds.graph.project.cypher("
                "$graph_name, "
                "$node_query, "
                "$relationship_query, "
                "{readConcurrency: $read_concurrency, parameters: { nodes: $nodes, relationships: $relationships }})"
            )

            node_query, nodes = self._node_query(node_df)
            relationship_query, relationships = self._relationship_query(relationship_df)

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

        def _node_query(self, node_df: DataFrame) -> Tuple[str, List[List[Any]]]:
            node_list = node_df.values.tolist()
            node_columns = list(node_df.columns)
            node_id_index = node_columns.index("nodeId")

            label_query = ""
            if "labels" in node_df.keys():
                label_index = node_columns.index("labels")
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
                property_queries = (f", node[{node_columns.index(col)}] as {col}" for col in property_columns)
                property_query = "".join(property_queries)

            return f"UNWIND $nodes as node RETURN node[{node_id_index}] as id{label_query}{property_query}", node_list

        def _relationship_query(self, rel_df: DataFrame) -> Tuple[str, List[List[Any]]]:
            rel_list = rel_df.values.tolist()
            rel_columns = list(rel_df.columns)
            source_id_index = rel_columns.index("sourceNodeId")
            target_id_index = rel_columns.index("targetNodeId")

            type_query = ""
            if "relationshipType" in rel_df.keys():
                type_index = rel_columns.index("relationshipType")
                type_query = f", relationship[{type_index}] as type"

            property_query = ""
            property_columns: Set[str] = set(rel_df.keys()) - {"sourceNodeId", "targetNodeId", "relationshipType"}
            if len(property_columns) > 0:
                property_queries = (f", relationship[{rel_columns.index(col)}] as {col}" for col in property_columns)
                property_query = "".join(property_queries)

            return (
                "UNWIND $relationships as relationship "
                f"RETURN relationship[{source_id_index}] as source, relationship[{target_id_index}] as target"
                f"{type_query}{property_query}",
                rel_list,
            )
