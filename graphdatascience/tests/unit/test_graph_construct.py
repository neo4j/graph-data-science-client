import pytest
from pandas import DataFrame

from .conftest import CollectingQueryRunner
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.server_version.server_version import ServerVersion


@pytest.mark.parametrize("server_version", [ServerVersion(2, 1, 0)])
def test_graph_project_based_construct_without_arrow(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
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

    gds.graph.construct("hello", nodes, relationships, concurrency=2)

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


@pytest.mark.parametrize(
    "server_version, tier, target_node_labels, target_node_properties, properties_key, in_between_configs",
    [
        (ServerVersion(2, 3, 0), ".alpha", "", "", "properties", "}, {"),
        (
            ServerVersion(2, 4, 0),
            "",
            ", targetNodeLabels: NULL",
            ", targetNodeProperties: NULL",
            "relationshipProperties",
            ", ",
        ),
    ],
    ids=["2.3.0 - Alpha Cypher Aggregation", "2.4.0 - New Cypher projection"],
)
def test_multi_df(
    runner: CollectingQueryRunner,
    gds: GraphDataScience,
    tier: str,
    target_node_labels: str,
    target_node_properties: str,
    properties_key: str,
    in_between_configs: str,
) -> None:
    nodes = [
        DataFrame({"nodeId": [0, 1], "labels": ["a", "a"], "property": [6.0, 7.0]}),
        DataFrame({"nodeId": [2, 3], "labels": ["b", "b"], "q": [-500, -400]}),
    ]
    relationships = [
        DataFrame(
            {"sourceNodeId": [0, 1], "targetNodeId": [1, 2], "relationshipType": ["A", "A"], "weights": [0.2, 0.3]}
        ),
        DataFrame({"sourceNodeId": [2, 3], "targetNodeId": [3, 0], "relationshipType": ["B", "B"]}),
    ]

    gds.graph.construct("hello", nodes, relationships)

    expected_proc_query = (
        "UNWIND $data AS data"
        " WITH data,"
        " CASE WHEN data[6] THEN data[5] ELSE null END AS sourceNodeLabels,"
        " CASE WHEN data[3] THEN data[2] ELSE null END AS relationshipType,"
        " CASE WHEN data[10] THEN data[9] ELSE null END AS targetNodeId,"
        " CASE WHEN data[8] THEN data[7] ELSE null END AS sourceNodeProperties,"
        f" CASE WHEN data[1] THEN data[0] ELSE null END AS {properties_key}"
        f" RETURN gds{tier}.graph.project("
        "$graph_name, data[4], targetNodeId, {"
        f"sourceNodeLabels: sourceNodeLabels{target_node_labels}, "
        f"sourceNodeProperties: sourceNodeProperties{target_node_properties}"
        f"{in_between_configs}"
        f"relationshipType: relationshipType, {properties_key}: {properties_key}"
        "}, $configuration)"
    )

    assert runner.last_query().replace("\n", "") == expected_proc_query

    actual_params = runner.last_params()

    expected_df = [
        [None, False, None, False, 0, "a", True, {"property": 6.0}, True, -1, False],
        [None, False, None, False, 1, "a", True, {"property": 7.0}, True, -1, False],
        [None, False, None, False, 2, "b", True, {"q": -500}, True, -1, False],
        [None, False, None, False, 3, "b", True, {"q": -400}, True, -1, False],
        [{"weights": 0.2}, True, "A", True, 0, None, False, None, False, 1, True],
        [{"weights": 0.3}, True, "A", True, 1, None, False, None, False, 2, True],
        [{}, True, "B", True, 2, None, False, None, False, 3, True],
        [{}, True, "B", True, 3, None, False, None, False, 0, True],
    ]

    assert actual_params == {
        "configuration": {"readConcurrency": 4, "undirectedRelationshipTypes": None},
        "graph_name": "hello",
        "data": expected_df,
    }


@pytest.mark.parametrize(
    "server_version, tier, target_node_labels, target_node_properties, properties_key, in_between_configs",
    [
        (ServerVersion(2, 3, 0), ".alpha", "", "", "properties", "}, {"),
        (
            ServerVersion(2, 4, 0),
            "",
            ", targetNodeLabels: NULL",
            ", targetNodeProperties: NULL",
            "relationshipProperties",
            ", ",
        ),
    ],
    ids=["2.3.0 - Alpha Cypher Aggregation", "2.4.0 - New Cypher projection"],
)
def test_graph_aggregation_based_construct_without_arrow(
    runner: CollectingQueryRunner,
    gds: GraphDataScience,
    tier: str,
    target_node_labels: str,
    target_node_properties: str,
    properties_key: str,
    in_between_configs: str,
) -> None:
    nodes = DataFrame(
        {
            "nodeId": [0, 1],
            "labels": [["A"], ["B"]],
            "pF": [1337, 42.42],
            "pI": [1337, 42],
            "pList": [[4, 5, 6, 7], [1, 2, 3]],
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

    gds.graph.construct("hello", nodes, relationships, concurrency=2, undirected_relationship_types=["REL"])

    expected_proc_query = (
        "UNWIND $data AS data"
        " WITH data,"
        " CASE WHEN data[6] THEN data[5] ELSE null END AS sourceNodeLabels,"
        " CASE WHEN data[3] THEN data[2] ELSE null END AS relationshipType,"
        " CASE WHEN data[10] THEN data[9] ELSE null END AS targetNodeId,"
        " CASE WHEN data[8] THEN data[7] ELSE null END AS sourceNodeProperties,"
        f" CASE WHEN data[1] THEN data[0] ELSE null END AS {properties_key}"
        f" RETURN gds{tier}.graph.project("
        "$graph_name, data[4], targetNodeId, {"
        f"sourceNodeLabels: sourceNodeLabels{target_node_labels}, "
        f"sourceNodeProperties: sourceNodeProperties{target_node_properties}"
        f"{in_between_configs}"
        f"relationshipType: relationshipType, {properties_key}: {properties_key}"
        "}, $configuration)"
    )

    # indices are based off the combined df
    assert runner.last_query().replace("\n", "") == expected_proc_query

    actual_params = runner.last_params()

    expected_df = [
        [None, False, None, False, 0, ["A"], True, {"pF": 1337.0, "pI": 1337, "pList": [4, 5, 6, 7]}, True, -1, False],
        [None, False, None, False, 1, ["B"], True, {"pF": 42.42, "pI": 42, "pList": [1, 2, 3]}, True, -1, False],
        [{"relPropA": 1337.2}, True, "REL", True, 0, None, False, None, False, 1, True],
        [{"relPropA": 42.0}, True, "REL2", True, 1, None, False, None, False, 0, True],
    ]

    assert actual_params == {
        "configuration": {"readConcurrency": 2, "undirectedRelationshipTypes": ["REL"]},
        "graph_name": "hello",
        "data": expected_df,
    }


@pytest.mark.parametrize("server_version", [ServerVersion(2, 3, 0)])
def test_graph_aggregation_based_construct_without_arrow_with_overlapping_property_columns(
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
        gds.graph.construct("hello", nodes, relationships, concurrency=2)


@pytest.mark.parametrize("server_version", [ServerVersion(2, 1, 0)])
def test_graph_construct_validate_df_columns(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    nodes = DataFrame({"nodeIds": [0, 1]})
    relationships = DataFrame({"sourceNodeId": [0, 1], "TargetNodeIds": [1, 0]})

    with pytest.raises(ValueError, match=r"(.*'nodeId'.*\s.*'targetNodeId'.*)|(.*'targetNodeId'.*\s.*'nodeId'.*)"):
        gds.graph.construct("hello", nodes, relationships, concurrency=2)


@pytest.mark.parametrize("server_version", [ServerVersion(2, 1, 0)])
def test_graph_alpha_construct_backward_compat(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
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

    with pytest.warns(DeprecationWarning):
        gds.alpha.graph.construct("hello", nodes, relationships, concurrency=2)
