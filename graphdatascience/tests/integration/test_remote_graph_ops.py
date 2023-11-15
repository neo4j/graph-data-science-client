from typing import Generator

import pytest

from graphdatascience.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.server_version.server_version import ServerVersion

GRAPH_NAME = "g"


@pytest.fixture(autouse=True, scope="class")
def run_around_tests(gds_with_cloud_setup: AuraGraphDataScience) -> Generator[None, None, None]:
    # Runs before each test
    auradb_runner.run_cypher(
        """
        CREATE
        (a: Node {x: 1, y: 2, z: [42], name: "nodeA"}),
        (b: Node {x: 2, y: 3, z: [1337], name: "nodeB"}),
        (c: Node {x: 3, y: 4, z: [9], name: "nodeC"}),
        (a)-[:REL {relX: 4, relY: 5}]->(b),
        (a)-[:REL {relX: 5, relY: 6}]->(c),
        (b)-[:REL {relX: 6, relY: 7}]->(c),
        (b)-[:REL2]->(c)
        """
    )

    yield  # Test runs here

    # Runs after each test
    gds_with_cloud_setup.run_cypher("MATCH (n) DETACH DELETE n")
    graph = gds_with_cloud_setup.graph.get(GRAPH_NAME)
    graph.drop(failIfMissing=False)


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 6, 0))
def test_remote_projection(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    G, result = gds_with_cloud_setup.graph.project.remoteDb(
        GRAPH_NAME, "MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)", "neo4j"
    )

    assert G.name() == GRAPH_NAME
    assert result["nodeCount"] == 3


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 6, 0))
def test_remote_write_back(gds_with_cloud_setup: AuraGraphDataScience) -> None:
    G, result = gds_with_cloud_setup.graph.project.remoteDb(
        GRAPH_NAME, "MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)", "neo4j"
    )

    result = gds_with_cloud_setup.pageRank.write(G, writeProperty="score")

    assert result["nodePropertiesWritten"] == 3
