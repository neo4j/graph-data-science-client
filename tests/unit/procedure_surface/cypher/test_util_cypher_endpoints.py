import pandas as pd
import pytest

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


def test_find_node_id_with_labels_and_properties(query_runner: CollectingQueryRunner) -> None:
    query_runner.add__mock_result("RETURN id(n) AS id", pd.DataFrame({"id": [42]}))
    util = UtilCypherEndpoints(query_runner)

    assert util.find_node_id(["City"], {"name": "New York City"}) == 42
    assert query_runner.last_query() == "MATCH (n) WHERE n:`City` AND n.`name` = $value_0 RETURN id(n) AS id"
    assert query_runner.last_params() == {"value_0": "New York City"}


def test_find_node_id_multiple_conditions(query_runner: CollectingQueryRunner) -> None:
    query_runner.add__mock_result("RETURN id(n) AS id", pd.DataFrame({"id": [7]}))
    util = UtilCypherEndpoints(query_runner)

    assert util.find_node_id(["City", "Capital"], {"settled": 1790, "name": "Washington D.C."}) == 7
    assert query_runner.last_query() == (
        "MATCH (n) WHERE n:`City` AND n:`Capital` AND n.`settled` = $value_0 AND n.`name` = $value_1 RETURN id(n) AS id"
    )
    assert query_runner.last_params() == {"value_0": 1790, "value_1": "Washington D.C."}


def test_find_node_id_no_filters(query_runner: CollectingQueryRunner) -> None:
    query_runner.add__mock_result("RETURN id(n) AS id", pd.DataFrame({"id": [0]}))
    util = UtilCypherEndpoints(query_runner)

    assert util.find_node_id() == 0
    assert query_runner.last_query() == "MATCH (n) RETURN id(n) AS id"
    assert query_runner.last_params() == {}


def test_find_node_id_raises_when_not_exactly_one_match(query_runner: CollectingQueryRunner) -> None:
    query_runner.add__mock_result("RETURN id(n) AS id", pd.DataFrame({"id": [1, 2]}))
    util = UtilCypherEndpoints(query_runner)

    with pytest.raises(ValueError, match="did not match with exactly one node"):
        util.find_node_id(["City"])
