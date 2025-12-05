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

    _, rels = gds.graph.networkx._parse(nx_G)

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


def test_parse_inconsistent_node_props(gds: GraphDataScience) -> None:
    nx_G = nx.Graph()
    nx_G.add_node(1, labels="N", time=1, date=2323)
    nx_G.add_node(42, labels="N", time=2, latitude=13.1)

    nodes, _ = gds.graph.networkx._parse(nx_G)

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


def test_parse_illegal_rel_type(gds: GraphDataScience) -> None:
    nx_G = nx.DiGraph()
    nx_G.add_node(1, labels="N")
    nx_G.add_node(42, labels="N")
    nx_G.add_edge(1, 42, relationshipType=3, weight=0.4)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "`relationshipType` edge attributes must be `None` or of type `str`. "
            "Given `relationshipType`: 3 for edge with source id 1 and target id 42"
        ),
    ):
        gds.graph.networkx._parse(nx_G)
