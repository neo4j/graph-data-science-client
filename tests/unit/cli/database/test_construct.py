import pandas as pd
import pytest
from gds_cli.database.construct import graph_from_construct_format


def test_graph_from_construct_format_basic() -> None:
    nodes = [
        pd.DataFrame([{"nodeId": 1, "labels": "Entity", "score": 1.0}]),
        pd.DataFrame([{"nodeId": 101, "labels": "Event"}]),
    ]
    relationships = [
        pd.DataFrame([{"sourceNodeId": 1, "targetNodeId": 101, "relationshipType": "HAS_EVENT"}]),
    ]

    graph = graph_from_construct_format(nodes, relationships)

    assert set(graph.node_dfs) == {"Entity", "Event"}
    assert list(graph.rel_dfs) == [("Entity", "HAS_EVENT", "Event")]
    assert len(graph.node_dfs["Entity"]) == 1
    assert len(graph.rel_dfs[("Entity", "HAS_EVENT", "Event")]) == 1


def test_graph_from_construct_format_default_relationship_type() -> None:
    nodes = [pd.DataFrame([{"nodeId": 1, "labels": "Node"}, {"nodeId": 2, "labels": "Node"}])]
    relationships = [pd.DataFrame([{"sourceNodeId": 1, "targetNodeId": 2}])]

    graph = graph_from_construct_format(nodes, relationships)

    assert list(graph.rel_dfs) == [("Node", "REL", "Node")]


def test_graph_from_construct_format_rejects_multi_label() -> None:
    # a hashable multi-label container (tuple) reaches label validation;
    # groupby itself would reject an unhashable one (e.g. a list) earlier.
    nodes = [pd.DataFrame([{"nodeId": 1, "labels": ("A", "B")}])]

    with pytest.raises(ValueError, match="single label string"):
        graph_from_construct_format(nodes, [])


def test_graph_from_construct_format_rejects_duplicate_node_id() -> None:
    nodes = [
        pd.DataFrame([{"nodeId": 1, "labels": "A"}]),
        pd.DataFrame([{"nodeId": 1, "labels": "B"}]),
    ]

    with pytest.raises(ValueError, match="not globally unique"):
        graph_from_construct_format(nodes, [])


def test_graph_from_construct_format_rejects_unresolved_endpoint() -> None:
    nodes = [pd.DataFrame([{"nodeId": 1, "labels": "A"}])]
    relationships = [pd.DataFrame([{"sourceNodeId": 1, "targetNodeId": 999}])]

    with pytest.raises(ValueError, match="unknown nodeIds"):
        graph_from_construct_format(nodes, relationships)
