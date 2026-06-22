import pandas as pd
from neo4j.graph import Graph, Node

from graphdatascience.procedure_surface.api.topological_link_prediction_endpoints import Direction
from graphdatascience.procedure_surface.cypher.topological_link_prediction_cypher_endpoints import (
    TopologicalLinkPredictionCypherEndpoints,
)
from graphdatascience.query_runner.query_mode import QueryMode
from tests.unit.conftest import CollectingQueryRunner


def _node(element_id: str, node_id: int) -> Node:
    return Node(Graph(), element_id, node_id, ["Label"], {})


def test_adamic_adar_with_node_ids(query_runner: CollectingQueryRunner) -> None:
    query_runner.add__mock_result("gds.linkprediction.adamicAdar", pd.DataFrame({"score": [1.5]}))
    endpoints = TopologicalLinkPredictionCypherEndpoints(query_runner)

    assert endpoints.adamic_adar(1, 2) == 1.5
    assert query_runner.last_query() == (
        """
        MATCH (n), (m)
        WHERE id(n) = $node1 AND id(m) = $node2
        RETURN gds.linkprediction.adamicAdar(n, m, $config) AS score
        """
    )
    assert query_runner.last_params() == {
        "node1": 1,
        "node2": 2,
        "config": {"direction": "BOTH", "relationshipQuery": None},
    }
    assert query_runner.last_run_args()["mode"] == QueryMode.READ


def test_adamic_adar_with_node_objects(query_runner: CollectingQueryRunner) -> None:
    query_runner.add__mock_result("gds.linkprediction.adamicAdar", pd.DataFrame({"score": [1.5]}))
    endpoints = TopologicalLinkPredictionCypherEndpoints(query_runner)

    assert endpoints.adamic_adar(_node("n1", 1), _node("n2", 2)) == 1.5
    assert query_runner.last_query() == (
        """
        MATCH (n), (m)
        WHERE id(n) = $node1 AND id(m) = $node2
        RETURN gds.linkprediction.adamicAdar(n, m, $config) AS score
        """
    )
    assert query_runner.last_params() == {
        "node1": 1,
        "node2": 2,
        "config": {"direction": "BOTH", "relationshipQuery": None},
    }


def test_mixed_node_id_and_object(query_runner: CollectingQueryRunner) -> None:
    query_runner.add__mock_result("gds.linkprediction.commonNeighbors", pd.DataFrame({"score": [3.0]}))
    endpoints = TopologicalLinkPredictionCypherEndpoints(query_runner)

    assert endpoints.common_neighbors(7, _node("n2", 2)) == 3.0
    assert query_runner.last_query() == (
        """
        MATCH (n), (m)
        WHERE id(n) = $node1 AND id(m) = $node2
        RETURN gds.linkprediction.commonNeighbors(n, m, $config) AS score
        """
    )
    assert query_runner.last_params() == {
        "node1": 7,
        "node2": 2,
        "config": {"direction": "BOTH", "relationshipQuery": None},
    }


def test_relationship_query_and_direction(query_runner: CollectingQueryRunner) -> None:
    query_runner.add__mock_result("gds.linkprediction.preferentialAttachment", pd.DataFrame({"score": [9.0]}))
    endpoints = TopologicalLinkPredictionCypherEndpoints(query_runner)

    result = endpoints.preferential_attachment(1, 2, relationship_query="FRIEND", direction=Direction.OUTGOING)
    assert result == 9.0
    assert query_runner.last_params() == {
        "node1": 1,
        "node2": 2,
        "config": {"direction": "OUTGOING", "relationshipQuery": "FRIEND"},
    }


def test_resource_allocation(query_runner: CollectingQueryRunner) -> None:
    query_runner.add__mock_result("gds.linkprediction.resourceAllocation", pd.DataFrame({"score": [0.25]}))
    endpoints = TopologicalLinkPredictionCypherEndpoints(query_runner)

    assert endpoints.resource_allocation(1, 2) == 0.25
    assert query_runner.last_query() == (
        """
        MATCH (n), (m)
        WHERE id(n) = $node1 AND id(m) = $node2
        RETURN gds.linkprediction.resourceAllocation(n, m, $config) AS score
        """
    )


def test_total_neighbors(query_runner: CollectingQueryRunner) -> None:
    query_runner.add__mock_result("gds.linkprediction.totalNeighbors", pd.DataFrame({"score": [5.0]}))
    endpoints = TopologicalLinkPredictionCypherEndpoints(query_runner)

    assert endpoints.total_neighbors(1, 2) == 5.0
    assert query_runner.last_query() == (
        """
        MATCH (n), (m)
        WHERE id(n) = $node1 AND id(m) = $node2
        RETURN gds.linkprediction.totalNeighbors(n, m, $config) AS score
        """
    )


def test_same_community(query_runner: CollectingQueryRunner) -> None:
    query_runner.add__mock_result("gds.linkprediction.sameCommunity", pd.DataFrame({"score": [1.0]}))
    endpoints = TopologicalLinkPredictionCypherEndpoints(query_runner)

    assert endpoints.same_community(1, 2) == 1.0
    assert query_runner.last_query() == (
        """
        MATCH (n), (m)
        WHERE id(n) = $node1 AND id(m) = $node2
        RETURN gds.linkprediction.sameCommunity(n, m, $communityProperty) AS score
        """
    )
    assert query_runner.last_params() == {"node1": 1, "node2": 2, "communityProperty": "community"}


def test_same_community_custom_property(query_runner: CollectingQueryRunner) -> None:
    query_runner.add__mock_result("gds.linkprediction.sameCommunity", pd.DataFrame({"score": [0.0]}))
    endpoints = TopologicalLinkPredictionCypherEndpoints(query_runner)

    assert endpoints.same_community(1, 2, community_property="partition") == 0.0
    assert query_runner.last_params()["communityProperty"] == "partition"
