import re

import pytest
from pandas import DataFrame
from pandas.testing import assert_frame_equal

from graphdatascience.graph_data_science import GraphDataScience

nx = pytest.importorskip("networkx")


def test_parse_with_everything(gds: GraphDataScience) -> None:
    nx_G = nx.Graph()
    nx_G.add_node(1, labels="N", time=1)
    nx_G.add_node(42, labels=["N", "M"], time=2)
    nx_G.add_node(1337, labels=["O"], time=3)
    nx_G.add_node(2, labels=["O"], time=10)
    nx_G.add_edge(1, 42, relationshipType="R", weight=0.4)
    nx_G.add_edge(1, 1337, relationshipType="R", weight=1.4)
    nx_G.add_edge(42, 1337, relationshipType="R", weight=0.1)

    nodes, rels = gds.graph.networkx._parse(nx_G)

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


def test_parse_without_types(gds: GraphDataScience) -> None:
    nx_G = nx.Graph()
    nx_G.add_node(1, labels="N", time=1)
    nx_G.add_node(42, labels=["N", "M"], time=2)
    nx_G.add_node(1337, labels=["O"], time=3)
    nx_G.add_node(2, labels=["O"], time=10)
    nx_G.add_edge(1, 42, weight=0.4)
    nx_G.add_edge(1, 1337, weight=1.4)
    nx_G.add_edge(42, 1337, weight=0.1)

    nodes, rels = gds.graph.networkx._parse(nx_G)

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


def test_parse_without_anything(gds: GraphDataScience) -> None:
    nx_G = nx.Graph()
    nx_G.add_node(1, time=1)
    nx_G.add_node(42, time=2)
    nx_G.add_node(1337, time=3)
    nx_G.add_node(2, time=10)
    nx_G.add_edge(1, 42, weight=0.4)
    nx_G.add_edge(1, 1337, weight=1.4)
    nx_G.add_edge(42, 1337, weight=0.1)

    nodes, rels = gds.graph.networkx._parse(nx_G)

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


def test_parse_inconsistent_rel_props(gds: GraphDataScience) -> None:
    nx_G = nx.DiGraph()
    nx_G.add_node(1, labels="N")
    nx_G.add_node(42, labels="N")
    nx_G.add_edge(1, 42, relationshipType="R", weight=0.4)
    nx_G.add_edge(42, 1, relationshipType="R", weight2=1.4)

    with pytest.raises(
        ValueError, match=re.escape("Not all relationships with type 'R' have the properties: ['weight', 'weight2']")
    ):
        gds.graph.networkx._parse(nx_G)

    nx_G = nx.DiGraph()
    nx_G.add_node(1, labels="N")
    nx_G.add_node(42, labels="N")
    nx_G.add_edge(1, 42, weight=0.4)
    nx_G.add_edge(42, 1, weight2=1.4)

    with pytest.raises(ValueError, match=re.escape("Not all relationships have the properties: ['weight', 'weight2']")):
        gds.graph.networkx._parse(nx_G)


def test_parse_inconsistent_node_props(gds: GraphDataScience) -> None:
    nx_G = nx.Graph()
    nx_G.add_node(1, labels="N", time=1, date=2323)
    nx_G.add_node(42, labels="N", time=2, latitude=13.1)

    with pytest.raises(
        ValueError, match=re.escape("Not all nodes with labels ['N'] have the properties: ['date', 'latitude']")
    ):
        gds.graph.networkx._parse(nx_G)


def test_parse_inconsistent_rel_types(gds: GraphDataScience) -> None:
    nx_G = nx.DiGraph()
    nx_G.add_node(1, labels="N")
    nx_G.add_node(42, labels="N")
    nx_G.add_edge(1, 42, weight=0.4)
    nx_G.add_edge(42, 1, relationshipType="R", weight2=1.4)

    with pytest.raises(ValueError, match="Some but not all edges have a 'relationshipType' attribute"):
        gds.graph.networkx._parse(nx_G)


def test_parse_inconsistent_node_labels(gds: GraphDataScience) -> None:
    nx_G = nx.DiGraph()
    nx_G.add_node(1)
    nx_G.add_node(42, labels="N")

    with pytest.raises(ValueError, match="Some but not all nodes have a 'labels' attribute"):
        gds.graph.networkx._parse(nx_G)


def test_parse_illegal_node_labels(gds: GraphDataScience) -> None:
    nx_G = nx.DiGraph()
    nx_G.add_node(42, labels=1337)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "`labels` node attributes must be of type `str` or `list[str]`. Given `labels`: 1337 for node with id 42"
        ),
    ):
        gds.graph.networkx._parse(nx_G)
