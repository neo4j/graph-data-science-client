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
    runner.run_query(f"CALL gds.graph.drop('{GRAPH_NAME}', false) YIELD graphName")


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


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_karate_club_graph_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G = gds_without_arrow.graph.load_karate_club()

    try:
        assert G.node_count() == 34
        assert G.relationship_count() == 78
    finally:
        G.drop()


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 3, 0))
def test_karate_club_graph_without_arrow_undirected(gds_without_arrow: GraphDataScience) -> None:
    G = gds_without_arrow.graph.load_karate_club(undirected=True)

    try:
        assert G.node_count() == 34
        assert G.relationship_count() == 156
    finally:
        G.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_karate_club_graph_with_arrow(gds: GraphDataScience) -> None:
    G = gds.graph.load_karate_club()

    try:
        assert G.node_count() == 34
        assert G.relationship_count() == 78
    finally:
        G.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 3, 0))
def test_karate_club_graph_with_arrow_undirected(gds: GraphDataScience) -> None:
    G = gds.graph.load_karate_club(undirected=True)

    try:
        assert G.node_count() == 34
        assert G.relationship_count() == 156
    finally:
        G.drop()


@pytest.mark.compatible_with(max_exclusive=ServerVersion(2, 3, 0))
def test_imdb_without_arrow_fails_before_23(gds_without_arrow: GraphDataScience) -> None:
    with pytest.raises(ValueError):
        gds_without_arrow.graph.load_imdb()


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 3, 0))
def test_imdb_graph_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G = gds_without_arrow.graph.load_imdb()

    try:
        assert G.node_count() == 12772
        assert G.relationship_count() == 37288
    finally:
        G.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 3, 0))
def test_imdb_graph_with_arrow(gds: GraphDataScience) -> None:
    G = gds.graph.load_imdb()

    try:
        assert G.node_count() == 12772
        assert G.relationship_count() == 37288
    finally:
        G.drop()


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 3, 0))
def test_lastfm_graph_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G = gds_without_arrow.graph.load_lastfm()

    try:
        assert G.node_count() == 19914
        assert G.relationship_count() == 584060

        assert set(G.node_labels()) == {"User", "Artist"}
        assert G.node_properties("User") == ["rawId"]
        assert G.node_properties("Artist") == ["rawId"]

        assert set(G.relationship_types()) == {"LISTEN_TO", "IS_FRIEND", "TAGGED"}
        assert set(G.relationship_properties("TAGGED")) == {"day", "month", "year", "tagID", "timestamp"}
        assert set(G.relationship_properties("LISTEN_TO")) == {"weight"}
        assert set(G.relationship_properties("IS_FRIEND")) == set()

    finally:
        G.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 3, 0))
def test_lastfm_graph_with_arrow(gds: GraphDataScience) -> None:
    G = gds.graph.load_lastfm()

    try:
        assert G.node_count() == 19914
        assert G.relationship_count() == 584060

        assert set(G.node_labels()) == {"User", "Artist"}
        assert G.node_properties("User") == ["rawId"]
        assert G.node_properties("Artist") == ["rawId"]

        assert set(G.relationship_types()) == {"LISTEN_TO", "IS_FRIEND", "TAGGED"}
        assert set(G.relationship_properties("TAGGED")) == {"day", "month", "year", "tagID", "timestamp"}
        assert set(G.relationship_properties("LISTEN_TO")) == {"weight"}
        assert set(G.relationship_properties("IS_FRIEND")) == set()
    finally:
        G.drop()


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 3, 0))
def test_lastfm_hetero_graph_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G = gds_without_arrow.graph.load_lastfm("lastfmtest", hetero_tag_rel=True)

    try:
        assert G.node_count() == 19914
        assert G.relationship_count() == 584060
        assert len(G.relationship_types()) == 2 + 9749

        assert set(G.node_labels()) == {"User", "Artist"}
        assert G.node_properties("User") == ["rawId"]
        assert G.node_properties("Artist") == ["rawId"]
    finally:
        G.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 3, 0))
def test_lastfm_hetero_graph_with_arrow(gds: GraphDataScience) -> None:
    G = gds.graph.load_lastfm("lastfmtest", hetero_tag_rel=True)

    try:
        assert G.node_count() == 19914
        assert G.relationship_count() == 584060
        assert len(G.relationship_types()) == 2 + 9749

        assert set(G.node_labels()) == {"User", "Artist"}
        assert G.node_properties("User") == ["rawId"]
        assert G.node_properties("Artist") == ["rawId"]
    finally:
        G.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
@pytest.mark.enterprise
def test_roundtrip_with_arrow(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "y"]}}, {"REL": {"properties": "relX"}})

    with pytest.warns(DeprecationWarning):
        rel_df = gds.graph.streamRelationshipProperty(G, "relX")
        node_df = gds.graph.streamNodeProperty(G, "x")

    G_2 = gds.graph.construct("arrowGraph", node_df, rel_df)

    try:
        assert G.node_count() == G_2.node_count()
        assert G.relationship_count() == G_2.relationship_count()
    finally:
        G_2.drop()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
@pytest.mark.enterprise
def test_roundtrip_with_arrow_22(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "y"]}}, {"REL": {"properties": "relX"}})

    rel_df = gds.graph.relationshipProperty.stream(G, "relX")
    node_df = gds.graph.nodeProperty.stream(G, "x")

    G_2 = gds.graph.construct("arrowGraph", node_df, rel_df)

    try:
        assert G.node_count() == G_2.node_count()
        assert G.relationship_count() == G_2.relationship_count()
    finally:
        G_2.drop()


@pytest.mark.encrypted_only
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_roundtrip_with_arrow_encrypted(gds_with_tls: GraphDataScience) -> None:
    G, _ = gds_with_tls.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "y"]}}, {"REL": {"properties": "relX"}})

    with pytest.warns(DeprecationWarning):
        rel_df = gds_with_tls.graph.streamRelationshipProperty(G, "relX")
        node_df = gds_with_tls.graph.streamNodeProperty(G, "x")

    G_2 = gds_with_tls.graph.construct("arrowGraph", node_df, rel_df)

    try:
        assert G.node_count() == G_2.node_count()
        assert G.relationship_count() == G_2.relationship_count()
    finally:
        G_2.drop()


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
def test_graph_construct_without_arrow(gds_without_arrow: GraphDataScience) -> None:
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

    G = gds_without_arrow.graph.construct("hello", nodes, relationships)

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
def test_graph_construct_undirected_without_arrow(gds_without_arrow: GraphDataScience) -> None:
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

    G = gds_without_arrow.graph.construct("hello", nodes, relationships, undirected_relationship_types=["REL2"])

    try:
        assert G.name() == "hello"
        assert G.node_count() == 4
        assert G.relationship_count() == 5
        assert set(G.relationship_types()) == {"REL", "REL2"}
    finally:
        G.drop()


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(max_exclusive=ServerVersion(2, 3, 0))
def warn_for_graph_construct_undirected_without_arrow(gds_without_arrow: GraphDataScience) -> None:
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
        gds_without_arrow.graph.construct("hello", nodes, relationships, undirected_relationship_types=["REL2"])


@pytest.mark.enterprise
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_construct_with_arrow(gds: GraphDataScience) -> None:
    nodes = DataFrame({"nodeId": [0, 1, 2, 3]})
    relationships = DataFrame({"sourceNodeId": [0, 1, 2, 3], "targetNodeId": [1, 2, 3, 0]})

    G = gds.graph.construct("hello", nodes, relationships)

    assert G.name() == "hello"
    assert G.node_count() == 4
    assert G.relationship_count() == 4

    G.drop()


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(max_exlusive=ServerVersion(2, 3, 0))
def warn_for_graph_construct_undirected_with_arrow(gds: GraphDataScience) -> None:
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
        gds.graph.construct("hello", nodes, relationships, undirected_relationship_types=["REL2"])


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
def test_error_on_construct_same_graph_twice(gds: GraphDataScience) -> None:
    nodes = DataFrame({"nodeId": [0, 1]})
    relationships = DataFrame({"sourceNodeId": [0, 1], "targetNodeId": [1, 0]})
    graph_name = "g"

    G = gds.graph.construct(graph_name, nodes, relationships, concurrency=2)

    try:
        with pytest.raises(ValueError, match=f"Graph '{graph_name}' already exists."):
            gds.graph.construct(graph_name, nodes, relationships, concurrency=2)
    finally:
        G.drop()


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

    G = gds.graph.construct("hello", nodes, relationships, undirected_relationship_types=["*"])

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

    G = gds.graph.construct("hello", nodes, relationships)

    assert G.name() == "hello"
    assert G.node_count() == 4
    assert G.relationship_count() == 4

    G.drop()


@pytest.mark.enterprise
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_construct_with_arrow_multiple_dfs(gds: GraphDataScience) -> None:
    nodes = [DataFrame({"nodeId": [0, 1]}), DataFrame({"nodeId": [2, 3]})]
    relationships = DataFrame({"sourceNodeId": [0, 1, 2, 3], "targetNodeId": [1, 2, 3, 0]})

    G = gds.graph.construct("hello", nodes, relationships)

    assert G.name() == "hello"
    assert G.node_count() == 4
    assert G.relationship_count() == 4

    G.drop()


@pytest.mark.enterprise
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_construct_without_arrow_enterprise_warning(gds_without_arrow: GraphDataScience) -> None:
    nodes = DataFrame({"nodeId": [0, 1, 2, 3]})
    relationships = DataFrame({"sourceNodeId": [0, 1, 2, 3], "targetNodeId": [1, 2, 3, 0]})

    with pytest.warns(UserWarning):
        G = gds_without_arrow.graph.construct("hello", nodes, relationships)
        G.drop()


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 3, 0))
def test_graph_construct_without_arrow_multi_dfs(gds_without_arrow: GraphDataScience) -> None:
    nodes = [
        DataFrame({"nodeId": [0, 1], "labels": ["a", "a"], "property": [6.0, 7.0]}),
        DataFrame({"nodeId": [2, 3], "labels": ["b", "b"], "q": [-500, -400]}),
    ]
    relationships = [
        DataFrame({"sourceNodeId": [0, 1], "targetNodeId": [1, 2], "relationshipType": ["A", "A"]}),
        DataFrame({"sourceNodeId": [2, 3], "targetNodeId": [3, 0], "relationshipType": ["B", "B"]}),
    ]

    G = gds_without_arrow.graph.construct("hello", nodes, relationships)

    assert G.name() == "hello"
    assert G.node_count() == 4
    assert G.node_properties("a") == ["property"]
    assert G.node_properties("b") == ["q"]
    assert G.relationship_count() == 4
    assert G.relationship_properties("A") == []
    assert G.relationship_properties("B") == []

    G.drop()


@pytest.mark.enterprise
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_construct_with_arrow_abort(gds: GraphDataScience) -> None:
    bad_nodes = DataFrame({"bogus": [0, 1, 2, 3]})
    relationships = DataFrame({"sourceNodeId": [0, 1, 2, 3], "targetNodeId": [1, 2, 3, 0]})

    with pytest.raises(Exception):
        gds.graph.construct("hello", bad_nodes, relationships)

    good_nodes = DataFrame({"nodeId": [0, 1, 2, 3]})
    G = gds.graph.construct("hello", good_nodes, relationships)

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
        gds.graph.construct("hello", nodes, relationships)


@pytest.mark.filterwarnings("ignore: GDS Enterprise users can use Apache Arrow")
def test_graph_alpha_construct_backward_compat_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    nodes = DataFrame({"nodeId": [0, 1, 2, 3], "labels": [["A"], "B", ["C", "A"], ["D"]]})
    relationships = DataFrame(
        {"sourceNodeId": [0, 1, 2, 3], "targetNodeId": [1, 2, 3, 0], "relationshipType": ["REL", "REL", "REL", "REL2"]}
    )

    with pytest.warns(DeprecationWarning):
        gds_without_arrow.alpha.graph.construct("hello", nodes, relationships)


def test_nodes_only_without__arrow(gds_without_arrow: GraphDataScience) -> None:
    nodes = DataFrame({"nodeId": [0], "labels": ["person"]})
    relationships = DataFrame({"sourceNodeId": [], "targetNodeId": [], "relationshipType": []})

    with pytest.warns(UserWarning):
        G = gds_without_arrow.graph.construct(graph_name="my_graph", nodes=nodes, relationships=relationships)

    try:
        assert G.node_count() == 1
        assert G.node_properties("person") == []
    finally:
        G.drop()


@pytest.mark.enterprise
def test_nodes_only(gds: GraphDataScience) -> None:
    nodes = DataFrame({"nodeId": [0], "labels": ["person"]})
    relationships = DataFrame({"sourceNodeId": [], "targetNodeId": [], "relationshipType": []})

    G = gds.graph.construct(graph_name="my_graph", nodes=nodes, relationships=relationships)

    assert G.node_count() == 1
    assert G.node_properties("person") == []


@pytest.mark.enterprise
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 1, 0))
def test_graph_alpha_construct_backward_compat_with_arrow(gds: GraphDataScience) -> None:
    nodes = DataFrame({"nodeId": [0, 1, 2, 3]})
    relationships = DataFrame({"sourceNodeId": [0, 1, 2, 3], "targetNodeId": [1, 2, 3, 0]})

    with pytest.warns(DeprecationWarning):
        gds.alpha.graph.construct("hello", nodes, relationships)
