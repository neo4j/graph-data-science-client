import pytest
from pandas import DataFrame

from .conftest import CollectingQueryRunner
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.server_version.server_version import ServerVersion


@pytest.mark.parametrize("server_version", [ServerVersion(2, 1, 0)])
def test_graph_project_based_alpha_construct_without_arrow(
    runner: CollectingQueryRunner, gds: GraphDataScience
) -> None:
    nodes = DataFrame(
        {
            "nodeId": [0, 1],
            "labels": [["A"], ["B"]],
            "propA": [1337, 42.1],
        }
    )
    relationships = DataFrame(
        {
            "sourceNodeId": [0, 1],
            "targetNodeId": [1, 0],
            "relationshipType": ["REL", "REL2"],
            "relPropA": [1337.2, 42],
        }
    )

    gds.alpha.graph.construct("hello", nodes, relationships, concurrency=2)

    expected_node_query = "UNWIND $nodes as node RETURN node[0] as id, node[1] as labels, node[2] as propA"
    expected_relationship_query = (
        "UNWIND $relationships as relationship RETURN "
        "relationship[0] as source, relationship[1] as target, "
        "relationship[2] as type, relationship[3] as relPropA"
    )
    expected_proc_query = (
        "CALL gds.graph.project.cypher("
        "$graph_name, "
        "$node_query, "
        "$relationship_query, "
        "{readConcurrency: $read_concurrency, parameters: { nodes: $nodes, relationships: $relationships }})"
    )

    assert runner.last_query() == expected_proc_query
    assert runner.last_params() == {
        "nodes": nodes.values.tolist(),
        "relationships": relationships.values.tolist(),
        "read_concurrency": 2,
        "graph_name": "hello",
        "node_query": expected_node_query,
        "relationship_query": expected_relationship_query,
    }


@pytest.mark.parametrize("server_version", [ServerVersion(2, 3, 0)])
def test_graph_aggregation_based_alpha_construct_without_arrow(
    runner: CollectingQueryRunner, gds: GraphDataScience
) -> None:
    nodes = DataFrame(
        {
            "nodeId": [0, 1],
            "labels": [["A"], ["B"]],
            "propA": [1337, 42.1],
        }
    )
    relationships = DataFrame(
        {
            "sourceNodeId": [0, 1],
            "targetNodeId": [1, 0],
            "relationshipType": ["REL", "REL2"],
            "relPropA": [1337.2, 42],
        }
    )

    gds.alpha.graph.construct("hello", nodes, relationships, concurrency=2)

    expected_proc_query = (
        "UNWIND $data AS data RETURN gds.alpha.graph.project("
        "$graph_name, data[$sourceNodeIdx], data[$targetNodeIdx], $nodesConfig, $relationshipsConfig, $configuration)"
    )

    # indices are based off the combined df
    assert runner.last_query() == expected_proc_query
    assert runner.last_params() == {
        "sourceNodeIdx": 2,
        "targetNodeIdx": 3,
        "nodesConfig": {"sourceNodeLabels": "data[0]", "sourceNodeProperties": {"propA": "data[1]"}},
        "relationshipsConfig": {"relationshipType": "data[4]", "properties": {"relPropA": "data[5]"}},
        "configuration": {"readConcurrency": 2},
        "graph_name": "hello",
    }


@pytest.mark.parametrize("server_version", [ServerVersion(2, 3, 0)])
def test_graph_aggregation_based_alpha_construct_without_arrow_with_overlapping_property_columns(
    runner: CollectingQueryRunner, gds: GraphDataScience
) -> None:
    nodes = DataFrame(
        {
            "nodeId": [0, 1],
            "labels": [["A"], ["B"]],
            "propA": [1337, 42.1],
        }
    )
    relationships = DataFrame(
        {
            "sourceNodeId": [0, 1],
            "targetNodeId": [1, 0],
            "relationshipType": ["REL", "REL2"],
            "propA": [1337.2, 42],
        }
    )

    gds.alpha.graph.construct("hello", nodes, relationships, concurrency=2)

    expected_proc_query = (
        "UNWIND $data AS data RETURN gds.alpha.graph.project("
        "$graph_name, data[$sourceNodeIdx], data[$targetNodeIdx], $nodesConfig, $relationshipsConfig, $configuration)"
    )

    # indices are based off the combined df
    assert runner.last_query() == expected_proc_query
    assert runner.last_params() == {
        "sourceNodeIdx": 2,
        "targetNodeIdx": 3,
        "nodesConfig": {"sourceNodeLabels": "data[0]", "sourceNodeProperties": {"propA": "data[1]"}},
        "relationshipsConfig": {"relationshipType": "data[4]", "properties": {"propA": "data[5]"}},
        "configuration": {"readConcurrency": 2},
        "graph_name": "hello",
    }


@pytest.mark.parametrize("server_version", [ServerVersion(2, 1, 0)])
def test_graph_alpha_construct_validate_df_columns(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    nodes = DataFrame({"nodeIds": [0, 1]})
    relationships = DataFrame({"sourceNodeId": [0, 1], "TargetNodeIds": [1, 0]})

    with pytest.raises(ValueError, match=r"(.*'nodeId'.*\s.*'targetNodeId'.*)|(.*'targetNodeId'.*\s.*'nodeId'.*)"):
        gds.alpha.graph.construct("hello", nodes, relationships, concurrency=2)
