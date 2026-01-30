import pytest
from pandas import DataFrame, concat

from graphdatascience.datasets.nx_loader import NXLoader
from tests.unit.datasets.collecting_graph_constructor import CollectingGraphConstructor

pytest.importorskip("networkx")

import networkx as nx


def test_undirected_nx() -> None:
    nx_G = nx.Graph()
    nx_G.add_node(1, labels=["N"], foo=1)
    nx_G.add_node(42, labels=["N", "M"], foo=2)
    nx_G.add_node(1337, labels=["N"], foo=3)
    nx_G.add_edge(1, 42, relationshipType="R", weight=0.4)
    nx_G.add_edge(1, 1337, relationshipType="R", weight=1.4)
    nx_G.add_edge(42, 1337, relationshipType="R", weight=1.1)

    graph_constructor = CollectingGraphConstructor()
    nx_loader = NXLoader(graph_constructor)

    G = nx_loader.load(nx_G, "g", concurrency=2)
    assert G.name() == "g"

    constructor_args = graph_constructor.calls["g"]
    nodes_dfs: list[DataFrame] = constructor_args["nodes"]
    relationships_dfs: list[DataFrame] = constructor_args["relationships"]

    assert constructor_args["concurrency"] == 2
    assert constructor_args["undirected_relationship_types"] == ["R"]

    assert len(nodes_dfs) == 2  # per label
    node_df = concat(nodes_dfs)
    assert len(node_df) == 3
    assert {n for n_labels in node_df["labels"].to_list() for n in n_labels} == {"N", "M"}
    assert node_df.columns.to_list() == ["labels", "nodeId", "foo"]

    assert len(relationships_dfs) == 1
    rel_df = relationships_dfs[0]
    assert len(rel_df) == 3
    assert rel_df.columns.to_list() == ["relationshipType", "sourceNodeId", "targetNodeId", "weight"]
    assert {r for r in rel_df["relationshipType"].to_list()} == {"R"}


def test_directed_nx() -> None:
    nx_G = nx.DiGraph()

    nx_G.add_node(1, labels=["N"], foo=1)
    nx_G.add_node(42, labels=["N", "M"], foo=2)
    nx_G.add_node(1337, labels=["N"], foo=3)
    nx_G.add_edge(1, 42, relationshipType="R", weight=0.4)
    nx_G.add_edge(1, 1337, relationshipType="R", weight=1.4)
    nx_G.add_edge(42, 1337, relationshipType="R", weight=1.1)

    graph_constructor = CollectingGraphConstructor()
    nx_loader = NXLoader(graph_constructor)
    G = nx_loader.load(nx_G, "g")

    constructor_args = graph_constructor.calls["g"]
    nodes_dfs: list[DataFrame] = constructor_args["nodes"]
    relationships_dfs: list[DataFrame] = constructor_args["relationships"]

    assert constructor_args["concurrency"] == 4
    assert constructor_args["undirected_relationship_types"] == []

    assert len(nodes_dfs) == 2  # per label
    node_df = concat(nodes_dfs)
    assert len(node_df) == 3
    assert {n for n_labels in node_df["labels"].to_list() for n in n_labels} == {"N", "M"}
    assert node_df.columns.to_list() == ["labels", "nodeId", "foo"]

    assert len(relationships_dfs) == 1
    rel_df = relationships_dfs[0]
    assert len(rel_df) == 3
    assert rel_df.columns.to_list() == ["relationshipType", "sourceNodeId", "targetNodeId", "weight"]
    assert {r for r in rel_df["relationshipType"].to_list()} == {"R"}
    assert G.name() == "g"
