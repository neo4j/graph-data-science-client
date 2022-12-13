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
            "propF": [1337, 42.42],
            "propI": [1337, 42],
            "propList": [[4, 5, 6, 7], [1, 2, 3]],
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

    gds.alpha.graph.construct("hello", nodes, relationships, concurrency=2, undirected_relationship_types=["REL"])

    expected_proc_query = (
        "UNWIND $data AS data"
        " WITH data,"
        " CASE data[12] WHEN true THEN data[11] ELSE null END AS targetNodeId,"
        " CASE data[1] WHEN true THEN data[0] ELSE null END AS propF,"
        " CASE data[3] WHEN true THEN data[2] ELSE null END AS propI,"
        " CASE data[5] WHEN true THEN data[4] ELSE null END AS propList,"
        " CASE data[7] WHEN true THEN data[6] ELSE null END AS relPropA"
        " RETURN gds.alpha.graph.project("
        "$graph_name, data[9], targetNodeId,"
        " {sourceNodeLabels: data[10], sourceNodeProperties: {propF: propF, propI: propI, propList: propList}},"
        " {relationshipType: data[8], properties: {relPropA: relPropA}}, $configuration)"
    )

    # indices are based off the combined df
    assert runner.last_query().replace("\n", "") == expected_proc_query

    actual_params = runner.last_params()
    print(actual_params["data"])

    expected_df = [
        [1337.0, True, 1337, True, [4, 5, 6, 7], True, 1337.2, False, None, 0, ["A"], -1, False],
        [42.42, True, 42, True, [1, 2, 3], True, 1337.2, False, None, 1, ["B"], -1, False],
        [1337.0, False, 1337, False, [], False, 1337.2, True, "REL", 0, None, 1, True],
        [1337.0, False, 1337, False, [], False, 42.0, True, "REL2", 1, None, 0, True],
    ]

    assert actual_params == {
        "configuration": {"readConcurrency": 2, "undirectedRelationshipTypes": ["REL"]},
        "graph_name": "hello",
        "data": expected_df,
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
            "propB": [1337, 42.1],
        }
    )
    relationships = DataFrame(
        {
            "sourceNodeId": [0, 1],
            "targetNodeId": [1, 0],
            "relationshipType": ["REL", "REL2"],
            "propA": [1337.2, 42],
            "propB": [1337.2, 42],
        }
    )

    with pytest.raises(ValueError, match="Expected disjoint column names in node and relationship df but the columns"):
        gds.alpha.graph.construct("hello", nodes, relationships, concurrency=2)


@pytest.mark.parametrize("server_version", [ServerVersion(2, 1, 0)])
def test_graph_alpha_construct_validate_df_columns(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    nodes = DataFrame({"nodeIds": [0, 1]})
    relationships = DataFrame({"sourceNodeId": [0, 1], "TargetNodeIds": [1, 0]})

    with pytest.raises(ValueError, match=r"(.*'nodeId'.*\s.*'targetNodeId'.*)|(.*'targetNodeId'.*\s.*'nodeId'.*)"):
        gds.alpha.graph.construct("hello", nodes, relationships, concurrency=2)
