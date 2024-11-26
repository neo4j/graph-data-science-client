from collections import defaultdict
from typing import Any, Optional

import networkx as nx
from pandas import DataFrame

from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion
from .graph_object import Graph


class NXLoader(UncallableNamespace, IllegalAttrChecker):
    @client_only_endpoint("gds.graph.networkx")
    @compatible_with("load", min_inclusive=ServerVersion(2, 1, 0))
    def load(self, nx_G: nx.Graph, graph_name: str, concurrency: int = 4) -> Graph:
        nodes, rels = self._parse(nx_G)

        undirected_rel_types = []
        if not isinstance(nx_G, nx_G.to_directed_class()):
            undirected_rel_types = [df["relationshipType"][0] for df in rels]

        constructor = self._query_runner.create_graph_constructor(graph_name, concurrency, undirected_rel_types)
        constructor.run(nodes, rels)

        return Graph(graph_name, self._query_runner)

    @staticmethod
    def _attr_to_labels_key(labels_attr: Any, node_id: Any, no_node_labels: Optional[bool]) -> tuple[str, ...]:
        node_labels: list[str]
        if no_node_labels:
            node_labels = ["N"]
        elif isinstance(labels_attr, str):
            node_labels = [labels_attr]
        elif isinstance(labels_attr, list):
            node_labels = labels_attr
        else:
            raise ValueError(
                "`labels` node attributes must be of type `str` or `list[str]`. "
                f"Given `labels`: {labels_attr} for node with id {node_id}"
            )

        return tuple(sorted(node_labels))

    @staticmethod
    def _parse_nodes(nx_G: nx.Graph) -> list[DataFrame]:
        node_dicts_by_labels: dict[tuple[str, ...], dict[str, list[Any]]] = defaultdict(lambda: defaultdict(list))
        node_props_schema: dict[tuple[str, ...], set[str]] = defaultdict(set)

        no_node_labels = None

        for node_id, attrs in nx_G.nodes(data=True):
            labels_attr = attrs.get("labels", None)

            if no_node_labels is None:
                no_node_labels = labels_attr in [None, []]

            if (labels_attr in [None, []]) is not no_node_labels:
                raise ValueError("Some but not all nodes have a 'labels' attribute")

            labels_key = NXLoader._attr_to_labels_key(labels_attr, node_id, no_node_labels)

            props = {prop for prop in attrs.keys() if prop != "labels"}

            node_props_schema[labels_key] = props.union(node_props_schema[labels_key])

        for node_id, attrs in nx_G.nodes(data=True):
            labels_key = NXLoader._attr_to_labels_key(attrs.get("labels", None), node_id, no_node_labels)

            node_dicts_by_labels[labels_key]["nodeId"].append(node_id)

            for prop in node_props_schema[labels_key]:
                node_dicts_by_labels[labels_key][prop].append(attrs.get(prop, None))

        return [
            DataFrame({"labels": [list(labels)] * len(attrs["nodeId"]), **attrs})
            for labels, attrs in node_dicts_by_labels.items()
        ]

    @staticmethod
    def _attr_to_type_key(type_attr: Optional[str]) -> str:
        return "R" if type_attr is None else type_attr

    @staticmethod
    def _parse_rels(nx_G: nx.Graph) -> list[DataFrame]:
        rel_dicts_by_types: dict[str, dict[str, list[Any]]] = defaultdict(lambda: defaultdict(list))
        rel_props_schema: dict[str, set[str]] = defaultdict(set)
        no_rel_types = None

        for edge_data in nx_G.edges(data=True):
            source_id, target_id, attrs = edge_data

            type_attr = attrs.get("relationshipType", None)
            if no_rel_types is None:
                no_rel_types = type_attr is None

            if (type_attr is None) is not no_rel_types:
                raise ValueError("Some but not all edges have a 'relationshipType' attribute")

            if type_attr and not isinstance(type_attr, str):
                raise ValueError(
                    "`relationshipType` edge attributes must be `None` or of type `str`. "
                    f"Given `relationshipType`: {type_attr} for edge with "
                    f"source id {source_id} and target id {target_id}"
                )

            type_key = NXLoader._attr_to_type_key(type_attr)

            props = {prop for prop in attrs.keys() if prop != "relationshipType"}

            rel_props_schema[type_key] = props.union(rel_props_schema[type_key])

        for edge_data in nx_G.edges(data=True):
            source_id, target_id, attrs = edge_data

            type_key = NXLoader._attr_to_type_key(attrs.get("relationshipType", None))

            rel_dicts_by_types[type_key]["sourceNodeId"].append(source_id)
            rel_dicts_by_types[type_key]["targetNodeId"].append(target_id)

            for prop in rel_props_schema[type_key]:
                rel_dicts_by_types[type_key][prop].append(attrs.get(prop, None))

        return [
            DataFrame({"relationshipType": [rel_type] * len(attrs["sourceNodeId"]), **attrs})
            for rel_type, attrs in rel_dicts_by_types.items()
        ]

    @staticmethod
    def _parse(nx_G: nx.Graph) -> tuple[list[DataFrame], list[DataFrame]]:
        nodes = NXLoader._parse_nodes(nx_G)
        rels = NXLoader._parse_rels(nx_G)

        return nodes, rels
