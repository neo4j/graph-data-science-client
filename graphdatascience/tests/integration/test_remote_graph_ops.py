from typing import Generator

import pytest

from graphdatascience import GraphDataScience
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.server_version.server_version import ServerVersion

GRAPH_NAME = "g"


@pytest.fixture(autouse=True)
def run_around_tests(auradb_runner: Neo4jQueryRunner) -> Generator[None, None, None]:
    # Runs before each test
    auradb_runner.run_query(
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

    print(auradb_runner.run_query("MATCH (n)-->(m) RETURN n as sourceNode, m as targetNode"))
    yield  # Test runs here

    # Runs after each test
    auradb_runner.run_query("MATCH (n) DETACH DELETE n")
    auradb_runner.run_query(f"CALL gds.graph.drop('{GRAPH_NAME}', false)")


@pytest.mark.cloud_architecture
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 4, 0))
def test_remote_projection(gds_with_cloud_setup: GraphDataScience) -> None:
    G, result = gds_with_cloud_setup.alpha.graph.project.remote(
        GRAPH_NAME, "MATCH (n)-->(m) RETURN n as sourceNode, m as targetNode", "neo4j"
    )

    assert G.name() == GRAPH_NAME
    assert result["nodeCount"] == 3
