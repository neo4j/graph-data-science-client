import itertools
import os
import warnings
from dataclasses import dataclass
from typing import Any, Optional
from uuid import uuid4

from pandas import DataFrame, concat

from ..server_version.server_version import ServerVersion
from .graph_constructor import GraphConstructor
from .query_runner import QueryRunner


class CypherProjectionApi:
    RELATIONSHIP_TYPE = "relationshipType"
    SOURCE_NODE_LABEL = "sourceNodeLabels"
    TARGET_NODE_LABEL = "targetNodeLabels"
    SOURCE_NODE_PROPERTIES = "sourceNodeProperties"
    TARGET_NODE_PROPERTIES = "targetNodeProperties"
    REL_PROPERTIES = "properties"
    REL_PROPERTIES_NEW = "relationshipProperties"


@dataclass
class EntityColumnSchema:
    all: set[str]
    properties: set[str]

    def has_labels(self) -> bool:
        return "labels" in self.all

    def has_properties(self) -> bool:
        return len(self.properties) > 0

    def has_rel_type(self) -> bool:
        return "relationshipType" in self.all


class GraphColumnSchema:
    def __init__(self, nodes: list[EntityColumnSchema], rels: list[EntityColumnSchema]):
        self.nodes_per_df = nodes
        self.rels_per_df = rels

        self.all_nodes = EntityColumnSchema(
            set().union(*[n.all for n in nodes]),
            set().union(*[n.properties for n in nodes]),
        )
        self.all_rels = EntityColumnSchema(
            set().union(*[r.all for r in rels]), set().union(*[r.properties for r in rels])
        )


class CypherGraphConstructor(GraphConstructor):
    def __init__(
        self,
        query_runner: QueryRunner,
        graph_name: str,
        concurrency: int,
        undirected_relationship_types: Optional[list[str]],
        server_version: ServerVersion,
    ):
        self._query_runner = query_runner
        self._concurrency = concurrency
        self._graph_name = graph_name
        self._server_version = server_version
        self._undirected_relationship_types = undirected_relationship_types

    def run(self, node_dfs: list[DataFrame], relationship_dfs: list[DataFrame]) -> None:
        if self._should_warn_about_arrow_missing():
            warnings.warn(
                "GDS Enterprise users can use Apache Arrow for fast graph construction; please see the documentation "
                "for instructions on how to enable it. Without Arrow enabled, this installation will use community "
                "edition graph construction (slower)"
            )

        # New Cypher propjection supports concurrency since 2.3.0
        if self._server_version >= ServerVersion(2, 3, 0):
            self.CypherProjectionRunner(
                self._query_runner,
                self._graph_name,
                self._concurrency,
                self._undirected_relationship_types,
                self._server_version,
            ).run(node_dfs, relationship_dfs)
        else:
            assert not self._undirected_relationship_types, "This should have been raised earlier."

            def graph_construct_error_multidf(element: str) -> str:
                return f"Graph construction only supports a single {element} dataframe on GDS versions prior to GDS 2.3"

            if len(node_dfs) > 1:
                raise ValueError(graph_construct_error_multidf("node"))

            if len(relationship_dfs) > 1:
                raise ValueError(graph_construct_error_multidf("relationship"))

            node_df = node_dfs[0]
            rel_df = relationship_dfs[0]

            self.LegacyCypherProjectionRunner(self._query_runner, self._graph_name, self._concurrency).run(
                node_df, rel_df
            )

    def _should_warn_about_arrow_missing(self) -> bool:
        try:
            license: str = self._query_runner.run_cypher(
                "CALL gds.debug.sysInfo() YIELD key, value WHERE key = 'gdsEdition' RETURN value", custom_error=False
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

    class CypherProjectionRunner:
        _BIT_COL_SUFFIX = "_is_present" + str(uuid4())

        def __init__(
            self,
            query_runner: QueryRunner,
            graph_name: str,
            concurrency: int,
            undirected_relationship_types: Optional[list[str]],
            server_version: ServerVersion,
        ):
            self._query_runner = query_runner
            self._concurrency = concurrency
            self._graph_name = graph_name
            self._undirected_relationship_types = undirected_relationship_types
            self._server_version = server_version

        def run(self, node_dfs: list[DataFrame], relationship_dfs: list[DataFrame]) -> None:
            graph_schema = self.schema(node_dfs, relationship_dfs)

            same_cols = graph_schema.all_rels.all.intersection(graph_schema.all_nodes.all)

            if same_cols:
                raise ValueError(
                    "Expected disjoint column names in node and relationship df "
                    f"but the columns {same_cols} exist in both dfs. Please rename the column in one df."
                )

            is_cypher_projection_v2 = self._server_version >= ServerVersion(2, 4, 0)

            rel_properties_key = (
                CypherProjectionApi.REL_PROPERTIES_NEW
                if is_cypher_projection_v2
                else CypherProjectionApi.REL_PROPERTIES
            )

            aligned_node_dfs = self.adjust_node_dfs(node_dfs, graph_schema, rel_properties_key)
            aligned_rel_dfs = self.adjust_rel_dfs(relationship_dfs, graph_schema, rel_properties_key)

            # concat instead of join as we want to first have all nodes and then the rels
            # this way we don't duplicate the node property data and its cheaper
            combined_df: DataFrame = concat(aligned_node_dfs + aligned_rel_dfs, ignore_index=True, copy=False)
            # sort the columns to ensure the order is the same (for testing)
            combined_df.sort_index(axis="columns", inplace=True)
            # using a list and not a set to preserve the order
            combined_cols: list[str] = combined_df.columns.tolist()

            property_clauses: list[str] = [
                self.check_value_clause(combined_cols, prop_col)
                for prop_col in [CypherProjectionApi.SOURCE_NODE_PROPERTIES, rel_properties_key]
            ]

            source_node_labels_clause = (
                self.check_value_clause(combined_cols, CypherProjectionApi.SOURCE_NODE_LABEL)
                if CypherProjectionApi.SOURCE_NODE_LABEL in combined_cols
                else ""
            )
            rel_type_clause = (
                self.check_value_clause(combined_cols, "relationshipType")
                if "relationshipType" in combined_cols
                else ""
            )
            target_id_clause = self.check_value_clause(combined_cols, "targetNodeId")

            nodes_config_part = self.nodes_config_part(graph_schema.nodes_per_df, is_cypher_projection_v2)
            rels_config_part = self.rels_config_part(graph_schema.rels_per_df, rel_properties_key)

            if is_cypher_projection_v2:
                data_config = f"{{{', '.join(itertools.chain(nodes_config_part, rels_config_part))}}}"
            else:
                data_config = f"{{{', '.join(nodes_config_part)}}}, {{{', '.join(rels_config_part)}}}"

            property_clauses_str = f"{os.linesep}" if len(property_clauses) > 0 else ""
            property_clauses_str += f"{os.linesep}".join(property_clauses)[:-2]  # remove the final comma

            tier = "" if is_cypher_projection_v2 else ".alpha"

            query = (
                "UNWIND $data AS data"
                f" WITH data, {source_node_labels_clause}{rel_type_clause}{target_id_clause}{property_clauses_str}"
                f" RETURN gds{tier}.graph.project("
                f"$graph_name, data[{combined_cols.index('sourceNodeId')}], targetNodeId, "
                f"{data_config}, $configuration)"
            )

            configuration = {
                "readConcurrency": self._concurrency,
                "undirectedRelationshipTypes": self._undirected_relationship_types,
            }

            self._query_runner.run_cypher(
                query,
                {
                    "data": combined_df.values.tolist(),
                    "graph_name": self._graph_name,
                    "configuration": configuration,
                },
                custom_error=False,
            )

        def check_value_clause(self, combined_cols: list[str], col: str) -> str:
            return (
                f"CASE"
                f" WHEN data[{combined_cols.index(col + self._BIT_COL_SUFFIX)}]"
                f" THEN data[{combined_cols.index(col)}]"
                f" ELSE null"
                f" END AS {col}, "
            )

        def schema(self, node_dfs: list[DataFrame], rel_dfs: list[DataFrame]) -> GraphColumnSchema:
            node_schema = []
            for df in node_dfs:
                node_cols = set(df.columns.tolist())
                node_schema.append(EntityColumnSchema(node_cols, node_cols - {"nodeId", "labels"}))

            rel_schema = []
            for df in rel_dfs:
                rel_cols = set(df.columns.tolist())
                rel_schema.append(
                    EntityColumnSchema(rel_cols, rel_cols - {"sourceNodeId", "targetNodeId", "relationshipType"})
                )

            return GraphColumnSchema(node_schema, rel_schema)

        def adjust_node_dfs(
            self, node_dfs: list[DataFrame], schema: GraphColumnSchema, rel_properties_key: str
        ) -> list[DataFrame]:
            adjusted_dfs = []

            for i, df in enumerate(node_dfs):
                node_dict: dict[str, Any] = {
                    "sourceNodeId": df["nodeId"],
                    "targetNodeId": -1,
                    f"targetNodeId{self._BIT_COL_SUFFIX}": False,
                }

                if CypherProjectionApi.RELATIONSHIP_TYPE in schema.all_rels.all:
                    node_dict[CypherProjectionApi.RELATIONSHIP_TYPE] = None
                    node_dict[CypherProjectionApi.RELATIONSHIP_TYPE + self._BIT_COL_SUFFIX] = False

                if "labels" in schema.nodes_per_df[i].all:
                    node_dict[CypherProjectionApi.SOURCE_NODE_LABEL + self._BIT_COL_SUFFIX] = True
                    node_dict[CypherProjectionApi.SOURCE_NODE_LABEL] = df["labels"]
                elif "labels" in schema.all_nodes.all:
                    node_dict[CypherProjectionApi.SOURCE_NODE_LABEL + self._BIT_COL_SUFFIX] = False
                    node_dict[CypherProjectionApi.SOURCE_NODE_LABEL] = ""

                def collect_to_dict(row: dict[str, Any]) -> dict[str, Any]:
                    return {column: row[column] for column in schema.nodes_per_df[i].properties}

                node_dict_df = DataFrame(node_dict)
                node_dict_df[CypherProjectionApi.SOURCE_NODE_PROPERTIES] = df.apply(collect_to_dict, axis=1)
                node_dict_df[CypherProjectionApi.SOURCE_NODE_PROPERTIES + self._BIT_COL_SUFFIX] = True
                node_dict_df[rel_properties_key] = None
                node_dict_df[rel_properties_key + self._BIT_COL_SUFFIX] = False

                adjusted_dfs.append(node_dict_df)

            return adjusted_dfs

        def adjust_rel_dfs(
            self, rel_dfs: list[DataFrame], schema: GraphColumnSchema, rel_properties_key: str
        ) -> list[DataFrame]:
            adjusted_dfs = []

            for i, df in enumerate(rel_dfs):
                rel_dict: dict[str, Any] = {
                    "sourceNodeId": df["sourceNodeId"],
                    "targetNodeId": df["targetNodeId"],
                    f"targetNodeId{self._BIT_COL_SUFFIX}": True,
                }

                if CypherProjectionApi.RELATIONSHIP_TYPE in schema.rels_per_df[i].all:
                    rel_dict[CypherProjectionApi.RELATIONSHIP_TYPE + self._BIT_COL_SUFFIX] = True
                    rel_dict[CypherProjectionApi.RELATIONSHIP_TYPE] = df[CypherProjectionApi.RELATIONSHIP_TYPE]
                elif CypherProjectionApi.RELATIONSHIP_TYPE in schema.all_rels.all:
                    rel_dict[CypherProjectionApi.RELATIONSHIP_TYPE + self._BIT_COL_SUFFIX] = False
                    rel_dict[CypherProjectionApi.RELATIONSHIP_TYPE] = None

                if "labels" in schema.all_nodes.all:
                    rel_dict[CypherProjectionApi.SOURCE_NODE_LABEL] = None
                    rel_dict[CypherProjectionApi.SOURCE_NODE_LABEL + self._BIT_COL_SUFFIX] = False

                def collect_to_dict(row: dict[str, Any]) -> dict[str, Any]:
                    return {column: row[column] for column in schema.rels_per_df[i].properties}

                rel_dict_df = DataFrame(rel_dict)
                rel_dict_df[rel_properties_key] = df.apply(collect_to_dict, axis=1)
                rel_dict_df[rel_properties_key + self._BIT_COL_SUFFIX] = True
                rel_dict_df[CypherProjectionApi.SOURCE_NODE_PROPERTIES] = None
                rel_dict_df[CypherProjectionApi.SOURCE_NODE_PROPERTIES + self._BIT_COL_SUFFIX] = False

                adjusted_dfs.append(rel_dict_df)

            return adjusted_dfs

        def nodes_config_part(self, node_cols: list[EntityColumnSchema], is_cypher_projection_v2: bool) -> list[str]:
            # Cannot use a dictionary as we need to refer to the `data` variable in the cypher query.
            # Otherwise we would just pass a string such as `data[0]`
            nodes_config_fields: list[str] = []
            if any(x.has_labels() for x in node_cols):
                nodes_config_fields.append(
                    f"{CypherProjectionApi.SOURCE_NODE_LABEL}: {CypherProjectionApi.SOURCE_NODE_LABEL}"
                )
                if is_cypher_projection_v2:
                    nodes_config_fields.append(
                        f"{CypherProjectionApi.TARGET_NODE_LABEL}: NULL",
                    )

            # as we first list all nodes at the top of the df, we don't need to lookup properties for the target node
            if any(x.has_properties() for x in node_cols):
                nodes_config_fields.append(
                    f"{CypherProjectionApi.SOURCE_NODE_PROPERTIES}: {CypherProjectionApi.SOURCE_NODE_PROPERTIES}"
                )
                if is_cypher_projection_v2:
                    nodes_config_fields.append(
                        f"{CypherProjectionApi.TARGET_NODE_PROPERTIES}: NULL",
                    )

            return nodes_config_fields

        def rels_config_part(self, rel_cols: list[EntityColumnSchema], rel_properties_key: str) -> list[str]:
            rels_config_fields: list[str] = []

            if any(x.has_rel_type() for x in rel_cols):
                rels_config_fields.append(
                    f"{CypherProjectionApi.RELATIONSHIP_TYPE}: {CypherProjectionApi.RELATIONSHIP_TYPE}"
                )

            if any(x.has_properties() for x in rel_cols):
                rels_config_fields.append(f"{rel_properties_key}: {rel_properties_key}")

            return rels_config_fields

    class LegacyCypherProjectionRunner:
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

            self._query_runner.run_cypher(
                query,
                {
                    "graph_name": self._graph_name,
                    "node_query": node_query,
                    "relationship_query": relationship_query,
                    "read_concurrency": self._concurrency,
                    "nodes": nodes,
                    "relationships": relationships,
                },
                custom_error=False,
            )

        def _node_query(self, node_df: DataFrame) -> tuple[str, list[list[Any]]]:
            # ignore type as tolist return depends on number of dimensions)
            node_list: list[list[Any]] = node_df.values.tolist()  # type: ignore
            node_columns = list(node_df.columns)
            node_id_index = node_columns.index("nodeId")

            label_query = ""
            if "labels" in node_df.keys():
                label_index = node_columns.index("labels")
                label_query = f", node[{label_index}] as labels"

                # Make sure every node has a list of labels
                for node in node_list:
                    labels = node[label_index]
                    if isinstance(labels, list):
                        continue
                    node[label_index] = [labels]

            property_query = ""
            property_columns: set[str] = set(node_df.columns.tolist()) - {"nodeId", "labels"}
            if len(property_columns) > 0:
                property_queries = (f", node[{node_columns.index(col)}] as {col}" for col in property_columns)
                property_query = "".join(property_queries)

            return f"UNWIND $nodes as node RETURN node[{node_id_index}] as id{label_query}{property_query}", node_list

        def _relationship_query(self, rel_df: DataFrame) -> tuple[str, list[list[Any]]]:
            rel_list: list[list[Any]] = rel_df.values.tolist()  # type: ignore
            rel_columns = list(rel_df.columns)
            source_id_index = rel_columns.index("sourceNodeId")
            target_id_index = rel_columns.index("targetNodeId")

            type_query = ""
            if "relationshipType" in rel_df.keys():
                type_index = rel_columns.index("relationshipType")
                type_query = f", relationship[{type_index}] as type"

            property_query = ""
            property_columns: set[str] = set(rel_df.columns.tolist()) - {
                "sourceNodeId",
                "targetNodeId",
                "relationshipType",
            }
            if len(property_columns) > 0:
                property_queries = (f", relationship[{rel_columns.index(col)}] as {col}" for col in property_columns)
                property_query = "".join(property_queries)

            return (
                "UNWIND $relationships as relationship "
                f"RETURN relationship[{source_id_index}] as source, relationship[{target_id_index}] as target"
                f"{type_query}{property_query}",
                rel_list,
            )
