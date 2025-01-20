from typing import Generator

import pytest

from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience

GRAPH_NAME = "g"


@pytest.fixture(autouse=True, scope="class")
def run_around_tests(gds_with_cloud_setup: AuraGraphDataScience) -> Generator[None, None, None]:
    # Runs before each test
    gds_with_cloud_setup.run_cypher(
        """
        CREATE
        (a: A:Node {x: 1, y: 2, z: [42], name: "nodeA"}),
        (b: B:Node {x: 2, y: 3, z: [1337], name: "nodeB"}),
        (c: C:Node {x: 3, y: 4, z: [9], name: "nodeC"}),
        (a)-[:REL {relX: 4, relY: 5}]->(b),
        (a)-[:REL {relX: 5, relY: 6}]->(c),
        (b)-[:REL {relX: 6, relY: 7}]->(c),
        (b)-[:REL2]->(c)
        """
    )

    yield  # Test runs here

    # Runs after each test
    gds_with_cloud_setup.run_cypher("MATCH (n) DETACH DELETE n")

    res = gds_with_cloud_setup.graph.list()
    for graph_name in res["graphName"]:
        gds_with_cloud_setup.graph.get(graph_name).drop(failIfMissing=True)


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_projection(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    G, result = gds_with_cloud_setup.graph.project(GRAPH_NAME, "MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)")

    assert G.name() == GRAPH_NAME
    assert result["nodeCount"] == 3


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_projection_and_writeback_custom_database_name(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    gds_with_cloud_setup.run_cypher("CREATE DATABASE test1234 IF NOT EXISTS")
    gds_with_cloud_setup.set_database("test1234")
    gds_with_cloud_setup.run_cypher("CREATE ()-[:T]->()")
    G, projection_result = gds_with_cloud_setup.graph.project(
        GRAPH_NAME, "MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)"
    )

    assert G.name() == GRAPH_NAME
    assert projection_result["nodeCount"] == 2
    assert projection_result["relationshipCount"] == 1

    write_result = gds_with_cloud_setup.wcc.write(G, writeProperty="wcc")

    assert write_result["nodePropertiesWritten"] == 2
    count_wcc_nodes_query = "MATCH (n WHERE n.wcc IS NOT NULL) RETURN count(*) AS c"
    nodes_with_wcc_custom_db = gds_with_cloud_setup.run_cypher(count_wcc_nodes_query).squeeze()
    assert nodes_with_wcc_custom_db == 2
    gds_with_cloud_setup.set_database("neo4j")
    # we get a warning because property wcc doesn't exist in the database -- which is good!
    with pytest.warns(RuntimeWarning):
        nodes_with_wcc_default_db = gds_with_cloud_setup.run_cypher(count_wcc_nodes_query).squeeze()
        assert nodes_with_wcc_default_db == 0


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_projection_with_small_batch_size(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    G, result = gds_with_cloud_setup.graph.project(
        GRAPH_NAME, "MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)", batch_size=10
    )

    assert G.name() == GRAPH_NAME
    assert result["nodeCount"] == 3


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_write_back_page_rank(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    G, result = gds_with_cloud_setup.graph.project(GRAPH_NAME, "MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)")

    result = gds_with_cloud_setup.pageRank.write(G, writeProperty="score")

    assert result["nodePropertiesWritten"] == 3


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_write_back_node_similarity(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    G, result = gds_with_cloud_setup.graph.project(GRAPH_NAME, "MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)")

    result = gds_with_cloud_setup.nodeSimilarity.write(
        G, writeRelationshipType="SIMILAR", writeProperty="score", similarityCutoff=0
    )

    assert result["relationshipsWritten"] == 2


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_write_back_node_properties(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    G, result = gds_with_cloud_setup.graph.project(GRAPH_NAME, "MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)")
    result = gds_with_cloud_setup.pageRank.mutate(G, mutateProperty="score")
    result = gds_with_cloud_setup.graph.nodeProperties.write(G, node_properties=["score"])

    assert result["propertiesWritten"] == 3


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_write_back_node_properties_with_multiple_labels(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    G, result = gds_with_cloud_setup.graph.project(
        GRAPH_NAME,
        "MATCH (n)-->(m) "
        "RETURN gds.graph.project.remote(n, m, {sourceNodeLabels: labels(n), targetNodeLabels: labels(m)})",
    )
    result = gds_with_cloud_setup.pageRank.write(G, writeProperty="score")

    assert result["nodePropertiesWritten"] == 3


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_write_back_node_properties_with_select_labels(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    G, result = gds_with_cloud_setup.graph.project(
        GRAPH_NAME,
        "MATCH (n)-->(m) "
        "RETURN gds.graph.project.remote(n, m, {sourceNodeLabels: labels(n), targetNodeLabels: labels(m)})",
    )
    result = gds_with_cloud_setup.pageRank.mutate(G, mutateProperty="score")
    result = gds_with_cloud_setup.graph.nodeProperties.write(G, "score", ["A"])

    assert result["propertiesWritten"] == 1


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_write_back_node_label(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    G, result = gds_with_cloud_setup.graph.project(GRAPH_NAME, "MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)")
    result = gds_with_cloud_setup.graph.nodeLabel.write(G, "Foo", nodeFilter="*")

    assert result["nodeLabelsWritten"] == 3


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_write_back_relationship_topology(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    G, result = gds_with_cloud_setup.graph.project(
        GRAPH_NAME, "MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m, {relationshipType: 'FOO'})"
    )
    result = gds_with_cloud_setup.graph.relationship.write(G, "FOO")

    assert result["relationshipsWritten"] == 4


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_write_back_relationship_property(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    G, result = gds_with_cloud_setup.graph.project(
        GRAPH_NAME,
        "MATCH (n)-->(m) "
        "RETURN gds.graph.project.remote(n, m, {relationshipType: 'FOO', relationshipProperties: {bar: 42}})",
    )
    result = gds_with_cloud_setup.graph.relationship.write(G, "FOO", "bar")

    assert result["relationshipsWritten"] == 4


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_write_back_relationship_properties(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    G, result = gds_with_cloud_setup.graph.project(
        GRAPH_NAME,
        "MATCH (n)-->(m) "
        "RETURN gds.graph.project.remote("
        "   n, "
        "   m, "
        "   {relationshipType: 'FOO', relationshipProperties: {bar: 42, foo: 1337}}"
        ")",
    )
    result = gds_with_cloud_setup.graph.relationshipProperties.write(G, "FOO", ["bar", "foo"])

    assert result["relationshipsWritten"] == 4


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_remote_write_back_relationship_property_from_pathfinding_algo(
    gds_with_cloud_setup: AuraGraphDataScience,
) -> None:
    G, result = gds_with_cloud_setup.graph.project(GRAPH_NAME, "MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)")

    source = gds_with_cloud_setup.find_node_id(properties={"x": 1})
    target = gds_with_cloud_setup.find_node_id(properties={"x": 2})
    result = gds_with_cloud_setup.shortestPath.dijkstra.write(
        G, sourceNode=source, targetNodes=target, writeRelationshipType="PATH", writeCosts=True
    )

    assert result["relationshipsWritten"] == 1


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 7, 0))
def test_empty_graph_write_back(
    gds_with_cloud_setup: AuraGraphDataScience,
) -> None:
    G, result = gds_with_cloud_setup.graph.project(
        GRAPH_NAME, "MATCH (n:MISSING) RETURN gds.graph.project.remote(n, null)"
    )

    assert G.node_count() == 0

    result = gds_with_cloud_setup.wcc.write(G, writeProperty="wcc")

    assert result["nodePropertiesWritten"] == 0
