from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple

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
        if not isinstance(nx_G, nx.DiGraph) and not isinstance(nx_G, nx.MultiDiGraph):
            undirected_rel_types = [df["relationshipType"][0] for df in rels]

        constructor = self._query_runner.create_graph_constructor(graph_name, concurrency, undirected_rel_types)
        constructor.run(nodes, rels)

        return Graph(graph_name, self._query_runner, self._server_version)

    @staticmethod
    def _parse(nx_G: nx.Graph) -> Tuple[List[DataFrame], List[DataFrame]]:
        node_dicts_by_labels: Dict[Tuple[str, ...], Dict[str, List[Any]]] = defaultdict(lambda: defaultdict(list))
        node_props_schema: Dict[Tuple[str, ...], Set[str]] = {}
        no_node_labels = None

        for node_id, attrs in nx_G.nodes(data=True):
            labels_attr = attrs.get("labels", None)
            if no_node_labels is None:
                no_node_labels = labels_attr in [None, []]

            if (labels_attr in [None, []]) is not no_node_labels:
                raise ValueError("Some but not all nodes have a 'labels' attribute")

            node_labels: List[str]
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

            labels_key: Tuple[str, ...] = tuple(sorted(node_labels))

            node_dicts_by_labels[labels_key]["nodeId"].append(node_id)

            props = {prop for prop in attrs.keys() if prop != "labels"}

            if labels_key in node_props_schema:
                if props != node_props_schema[labels_key]:
                    raise ValueError(
                        f"Not all nodes with labels {node_labels} have the properties: "
                        f"{sorted(list(props.symmetric_difference(node_props_schema[labels_key])))}"
                    )
            else:
                node_props_schema[labels_key] = props

            for prop in props:
                node_dicts_by_labels[labels_key][prop].append(attrs[prop])

        nodes = [
            DataFrame({"labels": [list(labels)] * len(attrs["nodeId"]), **attrs})
            for labels, attrs in node_dicts_by_labels.items()
        ]

        rel_dicts_by_types: Dict[str, Dict[str, List[Any]]] = defaultdict(lambda: defaultdict(list))
        rel_props_schema: Dict[str, Set[str]] = {}
        no_rel_types = None

        for edge_data in nx_G.edges(data=True):
            source_id, target_id, attrs = edge_data

            rel_type = attrs.get("relationshipType", None)
            if no_rel_types is None:
                no_rel_types = rel_type is None

            if (rel_type is None) is not no_rel_types:
                raise ValueError("Some but not all edges have a 'relationshipType' attribute")

            type_key = "R" if rel_type is None else rel_type

            rel_dicts_by_types[type_key]["sourceNodeId"].append(source_id)
            rel_dicts_by_types[type_key]["targetNodeId"].append(target_id)

            props = {prop for prop in attrs.keys() if prop != "relationshipType"}

            if type_key in rel_props_schema:
                if props != rel_props_schema[type_key]:
                    if rel_type is None:
                        raise ValueError(
                            f"Not all relationships have the properties: "
                            f"{sorted(list(props.symmetric_difference(rel_props_schema[type_key])))}"
                        )
                    else:
                        raise ValueError(
                            f"Not all relationships with type '{rel_type}' have the properties: "
                            f"{sorted(list(props.symmetric_difference(rel_props_schema[type_key])))}"
                        )
            else:
                rel_props_schema[type_key] = props

            for prop in props:
                rel_dicts_by_types[type_key][prop].append(attrs[prop])

        rels = [
            DataFrame({"relationshipType": [rel_type] * len(attrs["sourceNodeId"]), **attrs})
            for rel_type, attrs in rel_dicts_by_types.items()
        ]

        return nodes, rels
