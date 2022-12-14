from typing import Generator

import numpy as np
import pytest
from pandas import DataFrame

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.arrow_query_runner import ArrowQueryRunner
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.tests.integration.conftest import AUTH, URI

GRAPH_NAME = "g"


@pytest.fixture(autouse=True)
def run_around_tests(runner: Neo4jQueryRunner) -> Generator[None, None, None]:
    # Runs before each test
    runner.run_query(
        """
        CREATE
        (a: Node {x: 1, y: 2, z: [42]}),
        (b: Node {x: 2, y: 3, z: [1337]}),
        (c: Node {x: 3, y: 4, z: [9]}),
        (a)-[:REL {relX: 4, relY: 5}]->(b),
        (a)-[:REL {relX: 5, relY: 6}]->(c),
        (b)-[:REL {relX: 6, relY: 7}]->(c),
        (b)-[:REL2]->(c)
        """
    )

    yield  # Test runs here

    # Runs after each test
    runner.run_query("MATCH (n) DETACH DELETE n")
    runner.run_query(f"CALL gds.graph.drop('{GRAPH_NAME}', false)")


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_cora_graph_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G = gds_without_arrow.graph.load_cora()

    try:
        assert G.node_count() == 2708
        assert G.relationship_count() == 5429
    finally:
        G.drop()


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 3, 0))
def test_cora_graph_undirected_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G = gds_without_arrow.graph.load_cora(undirected=True)

    try:
        assert G.node_count() == 2708
        assert G.relationship_count() == 5429 * 2
    finally:
        G.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_cora_graph_with_arrow(gds: GraphDataScience) -> None:
    G = gds.graph.load_cora()

    try:
        assert G.node_count() == 2708
        assert G.relationship_count() == 5429
    finally:
        G.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_roundtrip_with_arrow(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "y"]}}, {"REL": {"properties": "relX"}})

    rel_df = gds.graph.streamRelationshipProperty(G, "relX")
    node_df = gds.graph.streamNodeProperty(G, "x")

    G_2 = gds.alpha.graph.construct("arrowGraph", node_df, rel_df)

    try:
        assert G.node_count() == G_2.node_count()
        assert G.relationship_count() == G_2.relationship_count()
    finally:
        G_2.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_roundtrip_with_arrow_22(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "y"]}}, {"REL": {"properties": "relX"}})

    rel_df = gds.graph.relationshipProperty.stream(G, "relX")
    node_df = gds.graph.nodeProperty.stream(G, "x")

    G_2 = gds.alpha.graph.construct("arrowGraph", node_df, rel_df)

    try:
        assert G.node_count() == G_2.node_count()
        assert G.relationship_count() == G_2.relationship_count()
    finally:
        G_2.drop()


@pytest.mark.encrypted_only
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_roundtrip_with_arrow_encrypted(gds_with_tls: GraphDataScience) -> None:
    G, _ = gds_with_tls.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "y"]}}, {"REL": {"properties": "relX"}})

    rel_df = gds_with_tls.graph.streamRelationshipProperty(G, "relX")
    node_df = gds_with_tls.graph.streamNodeProperty(G, "x")

    G_2 = gds_with_tls.alpha.graph.construct("arrowGraph", node_df, rel_df)

    try:
        assert G.node_count() == G_2.node_count()
        assert G.relationship_count() == G_2.relationship_count()
    finally:
        G_2.drop()


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_alpha_construct_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    nodes = DataFrame(
        {
            "nodeId": [0, 1, 2, 3],
            "labels": [["A"], "B", ["C", "A"], ["D"]],
            "propA": [1337, 42, 8, 133742],
            "propB": [1338, 43, 9, 133743],
            "propList": [[4, 5, 6, 7], [1, 2, 3], [8, 9], [10, 11]],
        }
    )
    relationships = DataFrame(
        {
            "sourceNodeId": [0, 1, 2, 3],
            "targetNodeId": [1, 2, 3, 0],
            "relationshipType": ["REL", "REL", "REL", "REL2"],
            "relPropA": [1337, 42, 8, 133742],
            "relPropB": [1338, 43, 9, 133743],
        }
    )

    G = gds_without_arrow.alpha.graph.construct("hello", nodes, relationships)

    try:
        assert G.name() == "hello"
        assert G.node_count() == 4
        assert G.relationship_count() == 4
        assert set(G.node_labels()) == {"A", "B", "C", "D"}
        assert set(G.relationship_types()) == {"REL", "REL2"}
        assert set(G.node_properties("A")) == {"propA", "propB", "propList"}
        assert set(G.relationship_properties("REL")) == {"relPropA", "relPropB"}
    finally:
        G.drop()


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 3, 0))
def test_graph_alpha_construct_undirected_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    nodes = DataFrame(
        {
            "nodeId": [0, 1, 2, 3],
        }
    )
    relationships = DataFrame(
        {
            "sourceNodeId": [0, 1, 2, 3],
            "targetNodeId": [1, 2, 3, 0],
            "relationshipType": ["REL", "REL", "REL", "REL2"],
            "relPropA": [1337, 42, 8, 133742],
        }
    )

    G = gds_without_arrow.alpha.graph.construct("hello", nodes, relationships, undirected_relationship_types=["REL2"])

    try:
        assert G.name() == "hello"
        assert G.node_count() == 4
        assert G.relationship_count() == 5
        assert set(G.relationship_types()) == {"REL", "REL2"}
    finally:
        G.drop()


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(max_exclusive=ServerVersion(2, 3, 0))
def warn_for_graph_alpha_construct_undirected_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    nodes = DataFrame(
        {
            "nodeId": [0, 1, 2, 3],
        }
    )
    relationships = DataFrame(
        {
            "sourceNodeId": [0, 1, 2, 3],
            "targetNodeId": [1, 2, 3, 0],
            "relationshipType": ["REL", "REL", "REL", "REL2"],
        }
    )

    with pytest.raises(ValueError):
        gds_without_arrow.alpha.graph.construct("hello", nodes, relationships, undirected_relationship_types=["REL2"])


@pytest.mark.enterprise
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_construct_with_arrow(gds: GraphDataScience) -> None:
    nodes = DataFrame({"nodeId": [0, 1, 2, 3]})
    relationships = DataFrame({"sourceNodeId": [0, 1, 2, 3], "targetNodeId": [1, 2, 3, 0]})

    G = gds.alpha.graph.construct("hello", nodes, relationships)

    assert G.name() == "hello"
    assert G.node_count() == 4
    assert G.relationship_count() == 4

    G.drop()


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(max_exlusive=ServerVersion(2, 3, 0))
def warn_for_graph_alpha_construct_undirected_with_arrow(gds: GraphDataScience) -> None:
    nodes = DataFrame(
        {
            "nodeId": [0, 1, 2, 3],
        }
    )
    relationships = DataFrame(
        {
            "sourceNodeId": [0, 1, 2, 3],
            "targetNodeId": [1, 2, 3, 0],
            "relationshipType": ["REL", "REL", "REL", "REL2"],
        }
    )

    with pytest.raises(ValueError):
        gds.alpha.graph.construct("hello", nodes, relationships, undirected_relationship_types=["REL2"])


@pytest.mark.enterprise
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 3, 0))
def test_graph_construct_undirected_with_arrow(gds: GraphDataScience) -> None:
    nodes = DataFrame({"nodeId": [0, 1, 2, 3]})
    relationships = DataFrame(
        {
            "sourceNodeId": [0, 1, 2, 3],
            "targetNodeId": [1, 2, 3, 0],
            "relPropA": [1337, 42, 8, 133742],
        }
    )

    G = gds.alpha.graph.construct("hello", nodes, relationships, undirected_relationship_types=["*"])

    try:
        assert G.name() == "hello"
        assert G.node_count() == 4
        assert G.relationship_count() == 8
    finally:
        G.drop()


@pytest.mark.enterprise
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 3, 0))
def test_graph_construct_with_nan_properties_with_arrow(gds: GraphDataScience) -> None:
    # pyarrow converts NaN and None in Dataframes both to `null`
    nodes = DataFrame(
        {
            "nodeId": [0, 1, 2, 3],
            "scalarProp": [42.42, np.nan, float("NaN"), None],
            "arrayProp": [[1.0, np.nan], [1.0, 2.0], [2.0, 2.0], [3.0, 3.0]],
        }
    )

    relationships = DataFrame(
        {"sourceNodeId": [0, 1, 2, 3], "targetNodeId": [1, 2, 3, 0], "scalarProp": [42.42, np.nan, float("NaN"), None]}
    )

    G = gds.alpha.graph.construct("hello", nodes, relationships)

    assert G.name() == "hello"
    assert G.node_count() == 4
    assert G.relationship_count() == 4

    G.drop()


@pytest.mark.enterprise
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_construct_with_arrow_multiple_dfs(gds: GraphDataScience) -> None:
    nodes = [DataFrame({"nodeId": [0, 1]}), DataFrame({"nodeId": [2, 3]})]
    relationships = DataFrame({"sourceNodeId": [0, 1, 2, 3], "targetNodeId": [1, 2, 3, 0]})

    G = gds.alpha.graph.construct("hello", nodes, relationships)

    assert G.name() == "hello"
    assert G.node_count() == 4
    assert G.relationship_count() == 4

    G.drop()


@pytest.mark.enterprise
@pytest.mark.skip_on_aura  # Should not warn when targeting AuraDS
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_construct_without_arrow_enterprise_warning(gds_without_arrow: GraphDataScience) -> None:
    nodes = DataFrame({"nodeId": [0, 1, 2, 3]})
    relationships = DataFrame({"sourceNodeId": [0, 1, 2, 3], "targetNodeId": [1, 2, 3, 0]})

    with pytest.warns(UserWarning):
        G = gds_without_arrow.alpha.graph.construct("hello", nodes, relationships)
        G.drop()


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_construct_without_arrow_multi_dfs(gds_without_arrow: GraphDataScience) -> None:
    nodes = DataFrame({"nodeId": [0, 1, 2, 3]})
    relationships = DataFrame({"sourceNodeId": [0, 1, 2, 3], "targetNodeId": [1, 2, 3, 0]})

    with pytest.raises(ValueError):
        gds_without_arrow.alpha.graph.construct("hello", [nodes, nodes], relationships)

    with pytest.raises(ValueError):
        gds_without_arrow.alpha.graph.construct("hello", nodes, [relationships, relationships])


@pytest.mark.enterprise
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_construct_with_arrow_abort(gds: GraphDataScience) -> None:
    bad_nodes = DataFrame({"bogus": [0, 1, 2, 3]})
    relationships = DataFrame({"sourceNodeId": [0, 1, 2, 3], "targetNodeId": [1, 2, 3, 0]})

    with pytest.raises(Exception):
        gds.alpha.graph.construct("hello", bad_nodes, relationships)

    good_nodes = DataFrame({"nodeId": [0, 1, 2, 3]})
    G = gds.alpha.graph.construct("hello", good_nodes, relationships)

    assert G.name() == "hello"
    assert G.node_count() == 4
    assert G.relationship_count() == 4

    G.drop()


@pytest.mark.enterprise
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_construct_with_arrow_no_db() -> None:
    gds = GraphDataScience(URI, auth=AUTH)
    if not isinstance(gds._query_runner, ArrowQueryRunner):
        pytest.skip("Arrow server not enabled")

    assert not gds.database()

    nodes = DataFrame({"nodeId": [0, 1, 2, 3]})
    relationships = DataFrame({"sourceNodeId": [0, 1, 2, 3], "targetNodeId": [1, 2, 3, 0]})

    with pytest.raises(ValueError):
        gds.alpha.graph.construct("hello", nodes, relationships)
