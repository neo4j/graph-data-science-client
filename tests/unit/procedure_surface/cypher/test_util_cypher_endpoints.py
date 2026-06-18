import pandas as pd

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.cypher.util_cypher_endpoints import UtilCypherEndpoints
from tests.unit.conftest import CollectingQueryRunner


def test_as_node(query_runner: CollectingQueryRunner) -> None:
    query_runner.add__mock_result("gds.util.asNode", pd.DataFrame({"n": ["node-1"]}))
    util = UtilCypherEndpoints(query_runner)

    assert util.as_node(1) == "node-1"
    assert query_runner.last_query() == "RETURN gds.util.asNode($nodeId)"
    assert query_runner.last_params() == {"nodeId": 1}


def test_as_nodes(query_runner: CollectingQueryRunner) -> None:
    query_runner.add__mock_result("gds.util.asNodes", pd.DataFrame({"nodes": [["node-1", "node-2"]]}))
    util = UtilCypherEndpoints(query_runner)

    assert util.as_nodes([1, 2]) == ["node-1", "node-2"]
    assert query_runner.last_query() == "RETURN gds.util.asNodes($nodeIds)"
    assert query_runner.last_params() == {"nodeIds": [1, 2]}


def test_node_property(query_runner: CollectingQueryRunner, graph: Graph) -> None:
    query_runner.add__mock_result("gds.util.nodeProperty", pd.DataFrame({"prop": [42]}))
    util = UtilCypherEndpoints(query_runner)

    assert util.node_property(graph, 1, "rank") == 42
    assert query_runner.last_query() == (
        "RETURN gds.util.nodeProperty($graph_name, $node_id, $property_key, $node_label)"
    )
    assert query_runner.last_params() == {
        "graph_name": "test_graph",
        "node_id": 1,
        "property_key": "rank",
        "node_label": "*",
    }


def test_one_hot_encoding_is_client_side(query_runner: CollectingQueryRunner) -> None:
    util = UtilCypherEndpoints(query_runner)

    assert util.one_hot_encoding(["a", "b", "c"], ["b"]) == [0, 1, 0]
    # Computed client-side: no query is issued.
    assert query_runner.queries == []


def test_one_hot_encoding_edge_cases(query_runner: CollectingQueryRunner) -> None:
    util = UtilCypherEndpoints(query_runner)

    assert util.one_hot_encoding([], []) == []
    assert util.one_hot_encoding(None, ["a"]) == []
    assert util.one_hot_encoding(["a", "b"], None) == [0, 0]
    assert util.one_hot_encoding(["a", "b", "a"], ["a"]) == [1, 0, 1]
