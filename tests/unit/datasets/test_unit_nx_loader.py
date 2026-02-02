import re

import pytest
from pandas import DataFrame, concat
from pandas._testing import assert_frame_equal

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

    assert "g" in graph_constructor.calls
    constructor_args = graph_constructor.calls["g"]
    nodes_dfs: list[DataFrame] = constructor_args["nodes"]
    relationships_dfs: list[DataFrame] = constructor_args["relationships"]

    assert constructor_args["concurrency"] is None
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


def test_parse_with_everything() -> None:
    nx_G = nx.Graph()
    nx_G.add_node(1, labels="N", time=1)
    nx_G.add_node(42, labels=["N", "M"], time=2)
    nx_G.add_node(1337, labels=["O"], time=3)
    nx_G.add_node(2, labels=["O"], time=10)
    nx_G.add_edge(1, 42, relationshipType="R", weight=0.4)
    nx_G.add_edge(1, 1337, relationshipType="R", weight=1.4)
    nx_G.add_edge(42, 1337, relationshipType="R", weight=0.1)

    graph_constructor = CollectingGraphConstructor()
    nx_loader = NXLoader(graph_constructor)
    nodes, rels = nx_loader._parse(nx_G)

    assert len(nodes) == 3
    assert_frame_equal(nodes[0], DataFrame({"labels": [["N"]], "nodeId": [1], "time": [1]}))
    assert_frame_equal(nodes[1], DataFrame({"labels": [["M", "N"]], "nodeId": [42], "time": [2]}))
    assert_frame_equal(nodes[2], DataFrame({"labels": [["O"], ["O"]], "nodeId": [1337, 2], "time": [3, 10]}))

    assert len(rels) == 1
    assert_frame_equal(
        rels[0],
        DataFrame(
            {
                "relationshipType": ["R"] * 3,
                "sourceNodeId": [1, 1, 42],
                "targetNodeId": [42, 1337, 1337],
                "weight": [0.4, 1.4, 0.1],
            }
        ),
    )


def test_parse_without_types() -> None:
    nx_G = nx.Graph()
    nx_G.add_node(1, labels="N", time=1)
    nx_G.add_node(42, labels=["N", "M"], time=2)
    nx_G.add_node(1337, labels=["O"], time=3)
    nx_G.add_node(2, labels=["O"], time=10)
    nx_G.add_edge(1, 42, weight=0.4)
    nx_G.add_edge(1, 1337, weight=1.4)
    nx_G.add_edge(42, 1337, weight=0.1)

    graph_constructor = CollectingGraphConstructor()
    nx_loader = NXLoader(graph_constructor)
    nodes, rels = nx_loader._parse(nx_G)

    assert len(nodes) == 3
    assert_frame_equal(nodes[0], DataFrame({"labels": [["N"]], "nodeId": [1], "time": [1]}))
    assert_frame_equal(nodes[1], DataFrame({"labels": [["M", "N"]], "nodeId": [42], "time": [2]}))
    assert_frame_equal(nodes[2], DataFrame({"labels": [["O"], ["O"]], "nodeId": [1337, 2], "time": [3, 10]}))

    assert len(rels) == 1
    assert_frame_equal(
        rels[0],
        DataFrame(
            {
                "relationshipType": ["R"] * 3,
                "sourceNodeId": [1, 1, 42],
                "targetNodeId": [42, 1337, 1337],
                "weight": [0.4, 1.4, 0.1],
            }
        ),
    )


def test_parse_without_anything() -> None:
    nx_G = nx.Graph()
    nx_G.add_node(1, time=1)
    nx_G.add_node(42, time=2)
    nx_G.add_node(1337, time=3)
    nx_G.add_node(2, time=10)
    nx_G.add_edge(1, 42, weight=0.4)
    nx_G.add_edge(1, 1337, weight=1.4)
    nx_G.add_edge(42, 1337, weight=0.1)

    graph_constructor = CollectingGraphConstructor()
    nx_loader = NXLoader(graph_constructor)
    nodes, rels = nx_loader._parse(nx_G)

    assert len(nodes) == 1
    assert_frame_equal(nodes[0], DataFrame({"labels": [["N"]] * 4, "nodeId": [1, 42, 1337, 2], "time": [1, 2, 3, 10]}))

    assert len(rels) == 1
    assert_frame_equal(
        rels[0],
        DataFrame(
            {
                "relationshipType": ["R"] * 3,
                "sourceNodeId": [1, 1, 42],
                "targetNodeId": [42, 1337, 1337],
                "weight": [0.4, 1.4, 0.1],
            }
        ),
    )


def test_parse_inconsistent_rel_props() -> None:
    nx_G = nx.DiGraph()
    nx_G.add_node(1, labels="N")
    nx_G.add_node(42, labels="N")
    nx_G.add_edge(1, 42, relationshipType="R", weight=0.4)
    nx_G.add_edge(42, 1, relationshipType="R", weight2=1.4)

    graph_constructor = CollectingGraphConstructor()
    nx_loader = NXLoader(graph_constructor)
    _, rels = nx_loader._parse(nx_G)

    assert len(rels) == 1
    assert_frame_equal(
        rels[0].sort_index(axis=1),
        DataFrame(
            {
                "relationshipType": ["R"] * 2,
                "sourceNodeId": [1, 42],
                "targetNodeId": [42, 1],
                "weight": [0.4, None],
                "weight2": [None, 1.4],
            }
        ).sort_index(axis=1),
    )


def test_parse_inconsistent_node_props() -> None:
    nx_G = nx.Graph()
    nx_G.add_node(1, labels="N", time=1, date=2323)
    nx_G.add_node(42, labels="N", time=2, latitude=13.1)

    graph_constructor = CollectingGraphConstructor()
    nx_loader = NXLoader(graph_constructor)
    nodes, _ = nx_loader._parse(nx_G)

    assert len(nodes) == 1
    assert_frame_equal(
        nodes[0].sort_index(axis=1),
        DataFrame(
            {
                "labels": [["N"], ["N"]],
                "nodeId": [1, 42],
                "time": [1, 2],
                "date": [2323, None],
                "latitude": [None, 13.1],
            }
        ).sort_index(axis=1),
    )


def test_parse_inconsistent_rel_types() -> None:
    nx_G = nx.DiGraph()
    nx_G.add_node(1, labels="N")
    nx_G.add_node(42, labels="N")
    nx_G.add_edge(1, 42, weight=0.4)
    nx_G.add_edge(42, 1, relationshipType="R", weight2=1.4)

    graph_constructor = CollectingGraphConstructor()
    nx_loader = NXLoader(graph_constructor)

    with pytest.raises(ValueError, match="Some but not all edges have a 'relationshipType' attribute"):
        nx_loader.load(nx_G, "g")


def test_parse_inconsistent_node_labels() -> None:
    nx_G = nx.DiGraph()
    nx_G.add_node(1)
    nx_G.add_node(42, labels="N")

    graph_constructor = CollectingGraphConstructor()
    nx_loader = NXLoader(graph_constructor)

    with pytest.raises(ValueError, match="Some but not all nodes have a 'labels' attribute"):
        nx_loader.load(nx_G, "g")


def test_parse_illegal_node_labels() -> None:
    nx_G = nx.DiGraph()
    nx_G.add_node(42, labels=1337)

    graph_constructor = CollectingGraphConstructor()
    nx_loader = NXLoader(graph_constructor)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "`labels` node attributes must be of type `str` or `list[str]`. Given `labels`: 1337 for node with id 42"
        ),
    ):
        nx_loader.load(nx_G, "g")


def test_parse_illegal_rel_type() -> None:
    nx_G = nx.DiGraph()
    nx_G.add_node(1, labels="N")
    nx_G.add_node(42, labels="N")
    nx_G.add_edge(1, 42, relationshipType=3, weight=0.4)

    graph_constructor = CollectingGraphConstructor()
    nx_loader = NXLoader(graph_constructor)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "`relationshipType` edge attributes must be `None` or of type `str`. "
            "Given `relationshipType`: 3 for edge with source id 1 and target id 42"
        ),
    ):
        nx_loader.load(nx_G, "g")
