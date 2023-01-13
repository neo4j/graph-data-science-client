from functools import reduce
from typing import Any, Dict, List, Type, Union

import pandas as pd
from pandas import DataFrame, Series

from ..utils.util_proc_runner import UtilProcRunner
from .graph_object import Graph
from .graph_type_check import graph_type_check
from graphdatascience.caller_base import CallerBase
from graphdatascience.error.illegal_attr_checker import IllegalAttrChecker
from graphdatascience.error.uncallable_namespace import UncallableNamespace
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.server_version.compatible_with import compatible_with
from graphdatascience.server_version.server_version import ServerVersion

Strings = Union[str, List[str]]


class TopologyDataFrame(DataFrame):
    @property
    def _constructor(self) -> "Type[TopologyDataFrame]":
        return TopologyDataFrame

    def by_rel_type(self) -> Dict[str, List[List[int]]]:
        gb = self.groupby("relationshipType")

        output = {}
        for rel_type, indices in gb.groups.items():
            one_rel_df = self.take(indices)
            output[str(rel_type)] = [list(one_rel_df["sourceNodeId"]), list(one_rel_df["targetNodeId"])]

        return output


class GraphEntityOpsBaseRunner(CallerBase, UncallableNamespace, IllegalAttrChecker):
    def __init__(self, query_runner: QueryRunner, namespace: str, server_version: ServerVersion):
        super().__init__(query_runner, namespace, server_version)
        self._util_proc_runner = UtilProcRunner(query_runner, "gds.util", server_version)

    @graph_type_check
    def _handle_properties(
        self,
        G: Graph,
        properties: Strings,
        entities: Strings,
        config: Dict[str, Any],
    ) -> DataFrame:
        query = f"CALL {self._namespace}($graph_name, $properties, $entities, $config)"
        params = {
            "graph_name": G.name(),
            "properties": properties,
            "entities": entities,
            "config": config,
        }

        return self._query_runner.run_query(query, params)


class GraphElementPropertyRunner(GraphEntityOpsBaseRunner):
    @compatible_with("stream", min_inclusive=ServerVersion(2, 2, 0))
    def stream(self, G: Graph, node_properties: str, node_labels: Strings = ["*"], **config: Any) -> DataFrame:
        self._namespace += ".stream"
        return self._handle_properties(G, node_properties, node_labels, config)


class GraphNodePropertiesRunner(GraphEntityOpsBaseRunner):
    @compatible_with("stream", min_inclusive=ServerVersion(2, 2, 0))
    def stream(
        self,
        G: Graph,
        node_properties: List[str],
        node_labels: Strings = ["*"],
        separate_property_columns: bool = False,
        db_node_properties: List[str] = [],
        **config: Any,
    ) -> DataFrame:
        self._namespace += ".stream"

        result = self._handle_properties(G, node_properties, node_labels, config)

        # new format was requested, but the query was run via Cypher
        if separate_property_columns and "propertyValue" in result.keys():
            result = result.pivot(index="nodeId", columns="nodeProperty", values="propertyValue")
            result = result.reset_index()
            result.columns.name = None
        # old format was requested but the query was run via Arrow
        elif not separate_property_columns and "propertyValue" not in result.keys():
            result = result.melt(id_vars=["nodeId"]).rename(
                columns={"variable": "nodeProperty", "value": "propertyValue"}
            )

        if db_node_properties:
            duplicate_properties = set(db_node_properties).intersection(set(node_properties))
            if duplicate_properties:
                raise ValueError(
                    f"Duplicate property keys '{duplicate_properties}' in db_node_properties and " f"node_properties."
                )

            unique_node_ids = result["nodeId"].drop_duplicates().tolist()
            db_properties_df = self._query_runner.run_query(
                self._build_query(db_node_properties), {"ids": unique_node_ids}
            )
            if "propertyValue" not in result.keys():
                result = result.join(db_properties_df.set_index("nodeId"), on="nodeId")
            else:
                db_properties_df = db_properties_df.melt(id_vars=["nodeId"]).rename(
                    columns={"variable": "nodeProperty", "value": "propertyValue"}
                )
                result = pd.concat([result, db_properties_df])

        return result

    @staticmethod
    def _build_query(db_node_properties: List[str]) -> str:
        query_prefix = "MATCH (n) WHERE id(n) IN $ids RETURN id(n) AS nodeId"

        def add_property(query: str, prop: str) -> str:
            return f"{query}, n.`{prop}` AS `{prop}`"

        return reduce(add_property, db_node_properties, query_prefix)

    @compatible_with("write", min_inclusive=ServerVersion(2, 2, 0))
    def write(self, G: Graph, node_properties: List[str], node_labels: Strings = ["*"], **config: Any) -> "Series[Any]":
        self._namespace += ".write"
        return self._handle_properties(G, node_properties, node_labels, config).squeeze()  # type: ignore

    @compatible_with("drop", min_inclusive=ServerVersion(2, 2, 0))
    @graph_type_check
    def drop(self, G: Graph, node_properties: List[str], **config: Any) -> "Series[Any]":
        self._namespace += ".drop"
        query = f"CALL {self._namespace}($graph_name, $properties, $config)"
        params = {
            "graph_name": G.name(),
            "properties": node_properties,
            "config": config,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore


class GraphRelationshipPropertiesRunner(GraphEntityOpsBaseRunner):
    @compatible_with("stream", min_inclusive=ServerVersion(2, 2, 0))
    def stream(
        self,
        G: Graph,
        relationship_properties: List[str],
        relationship_types: Strings = ["*"],
        separate_property_columns: bool = False,
        **config: Any,
    ) -> DataFrame:
        self._namespace += ".stream"

        result = self._handle_properties(G, relationship_properties, relationship_types, config)

        # new format was requested, but the query was run via Cypher
        if separate_property_columns and "propertyValue" in result.keys():
            result = result.pivot(
                index=["sourceNodeId", "targetNodeId", "relationshipType"],
                columns="relationshipProperty",
                values="propertyValue",
            )
            result = result.reset_index()
            result.columns.name = None
        # old format was requested but the query was run via Arrow
        elif not separate_property_columns and "propertyValue" not in result.keys():
            result = result.melt(id_vars=["sourceNodeId", "targetNodeId", "relationshipType"]).rename(
                columns={"variable": "relationshipProperty", "value": "propertyValue"}
            )

        return result


class GraphRelationshipRunner(GraphEntityOpsBaseRunner):
    @compatible_with("write", min_inclusive=ServerVersion(2, 2, 0))
    @graph_type_check
    def write(self, G: Graph, relationship_type: str, relationship_property: str = "", **config: Any) -> "Series[Any]":
        self._namespace += ".write"
        query = f"CALL {self._namespace}($graph_name, $relationship_type, $relationship_property, $config)"
        params = {
            "graph_name": G.name(),
            "relationship_type": relationship_type,
            "relationship_property": relationship_property,
            "config": config,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore


class ToUndirectedRunner(CallerBase, IllegalAttrChecker):
    def _run_procedure(
        self, G: Graph, query: str, relationship_type: str, mutate_relationship_type: str, **config: Any
    ) -> "Series[Any]":
        actual_config = {
            **config,
            "relationshipType": relationship_type,
            "mutateRelationshipType": mutate_relationship_type,
        }

        params = {"graph_name": G.name(), "config": actual_config}
        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    @graph_type_check
    def __call__(self, G: Graph, relationship_type: str, mutate_relationship_type: str, **config: Any) -> "Series[Any]":
        query = f"CALL {self._namespace}($graph_name, $config)"
        return self._run_procedure(G, query, relationship_type, mutate_relationship_type)

    @graph_type_check
    @compatible_with("estimate", min_inclusive=ServerVersion(2, 3, 0))
    def estimate(self, G: Graph, relationship_type: str, mutate_relationship_type: str, **config: Any) -> "Series[Any]":
        query = f"CALL {self._namespace}.estimate($graph_name, $config)"
        return self._run_procedure(G, query, relationship_type, mutate_relationship_type)


class GraphRelationshipsRunner(GraphEntityOpsBaseRunner):
    @compatible_with("drop", min_inclusive=ServerVersion(2, 2, 0))
    @graph_type_check
    def drop(
        self,
        G: Graph,
        relationship_type: str,
    ) -> "Series[Any]":
        self._namespace += ".drop"
        query = f"CALL {self._namespace}($graph_name, $relationship_type)"
        params = {
            "graph_name": G.name(),
            "relationship_type": relationship_type,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    @compatible_with("stream", min_inclusive=ServerVersion(2, 2, 0))
    @graph_type_check
    def stream(self, G: Graph, relationship_types: List[str] = ["*"], **config: Any) -> TopologyDataFrame:
        self._namespace += ".stream"
        query = f"CALL {self._namespace}($graph_name, $relationship_types, $config)"

        params = {"graph_name": G.name(), "relationship_types": relationship_types, "config": config}

        return TopologyDataFrame(self._query_runner.run_query(query, params))

    @property
    @compatible_with("toUndirected", min_inclusive=ServerVersion(2, 3, 0))
    def toUndirected(self) -> ToUndirectedRunner:
        self._namespace += ".toUndirected"
        return ToUndirectedRunner(self._query_runner, self._namespace, self._server_version)


class GraphPropertyRunner(CallerBase, UncallableNamespace, IllegalAttrChecker):
    @compatible_with("stream", min_inclusive=ServerVersion(2, 2, 0))
    @graph_type_check
    def stream(
        self,
        G: Graph,
        graph_property: str,
        **config: Any,
    ) -> DataFrame:
        self._namespace += ".stream"
        query = f"CALL {self._namespace}($graph_name, $graph_property, $config)"
        params = {"graph_name": G.name(), "graph_property": graph_property, "config": config}

        return self._query_runner.run_query(query, params)

    @compatible_with("drop", min_inclusive=ServerVersion(2, 2, 0))
    @graph_type_check
    def drop(
        self,
        G: Graph,
        graph_property: str,
        **config: Any,
    ) -> "Series[Any]":
        self._namespace += ".drop"
        query = f"CALL {self._namespace}($graph_name, $graph_property, $config)"
        params = {"graph_name": G.name(), "graph_property": graph_property, "config": config}

        return self._query_runner.run_query(query, params)  # type: ignore


class GraphLabelRunner(GraphEntityOpsBaseRunner):
    @compatible_with("write", min_inclusive=ServerVersion(2, 3, 0))
    @graph_type_check
    def write(self, G: Graph, node_label: str, **config: Any) -> "Series[Any]":
        self._namespace += ".write"
        query = f"CALL {self._namespace}($graph_name, $node_label, $config)"
        params = {
            "graph_name": G.name(),
            "node_label": node_label,
            "config": config,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    @compatible_with("mutate", min_inclusive=ServerVersion(2, 3, 0))
    @graph_type_check
    def mutate(self, G: Graph, node_label: str, **config: Any) -> "Series[Any]":
        self._namespace += ".mutate"
        query = f"CALL {self._namespace}($graph_name, $node_label, $config)"
        params = {
            "graph_name": G.name(),
            "node_label": node_label,
            "config": config,
        }

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore
