"""Build a random :class:`Graph` from a declarative config (vendored)."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd

from gds_cli.database.graph.graph import Graph, NodeIdMapping
from gds_cli.database.graph.random_data import RandomGenerator
from gds_cli.database.graph.random_edges import (
    RelationshipGenerator,
    UniformRelationshipGenerator,
)

NodeType = str
RelType = str
TripletType = tuple[NodeType, RelType, NodeType]


@dataclass
class RandomNodesConfig:
    node_count: int
    properties: dict[str, RandomGenerator]
    labels: list[str] | str | None = None


@dataclass
class RandomRelsConfig:
    rel_count: int
    rels: RelationshipGenerator
    properties: dict[str, RandomGenerator]


class RandomGraphConfig:
    def __init__(
        self,
        node_config: Mapping[NodeType, "int | RandomNodesConfig"],
        rel_config: Mapping[TripletType, "int | RandomRelsConfig"],
        nodeIdMapping: NodeIdMapping | None = None,
        include_labels_and_types: bool = False,
    ):
        nodes: dict[NodeType, RandomNodesConfig] = {}
        for node_type, node_args in node_config.items():
            if isinstance(node_args, RandomNodesConfig):
                nodes[node_type] = node_args
            elif isinstance(node_args, int):
                nodes[node_type] = RandomNodesConfig(node_count=node_args, properties=dict())

        rels: dict[TripletType, RandomRelsConfig] = {}
        for triplet_type, rel_args in rel_config.items():
            if isinstance(rel_args, RandomRelsConfig):
                rels[triplet_type] = rel_args
            elif isinstance(rel_args, int):
                rels[triplet_type] = RandomRelsConfig(
                    rel_count=rel_args, rels=UniformRelationshipGenerator(), properties=dict()
                )

        self.nodes = nodes
        self.rels = rels
        self.nodeIdMapping = nodeIdMapping
        self.include_labels_and_types = include_labels_and_types


def create_graph(rgc: RandomGraphConfig) -> Graph:
    # Fail fast: validate every relationship config before generating any data.
    for triplet, rrc in rgc.rels.items():
        rrc.rels.check(rgc.nodes[triplet[0]].node_count, rgc.nodes[triplet[2]].node_count, rrc.rel_count)

    node_dfs = {}
    for node_type, rnc in rgc.nodes.items():
        df = pd.DataFrame.from_dict(
            {"nodeId": range(rnc.node_count)}
            | {property_name: generator.generate(rnc.node_count) for property_name, generator in rnc.properties.items()}
        )
        if rgc.include_labels_and_types:
            if rnc.labels is None:
                df["labels"] = node_type  # type: ignore[assignment]
            else:
                extra_labels = [rnc.labels] if isinstance(rnc.labels, str) else rnc.labels
                df["labels"] = [list(set([node_type] + extra_labels))] * df.shape[0]
        node_dfs[node_type] = df

    rel_dfs = {}
    for triplet, rrc in rgc.rels.items():
        source_node_count = rgc.nodes[triplet[0]].node_count
        target_node_count = rgc.nodes[triplet[2]].node_count
        src, tgt = rrc.rels.generate_edges(source_node_count, target_node_count, rrc.rel_count)
        rel_dict: dict[str, Any] = {
            "sourceNodeId": src,
            "targetNodeId": tgt,
        }
        if rgc.include_labels_and_types:
            rel_dict["relationshipType"] = triplet[1]
        rel_dfs[triplet] = pd.DataFrame.from_dict(
            rel_dict
            | {property_name: generator.generate(rrc.rel_count) for property_name, generator in rrc.properties.items()}
        )

    graph = Graph(node_dfs, rel_dfs)
    if rgc.nodeIdMapping:
        graph.remap_node_ids(rgc.nodeIdMapping)
    return graph
