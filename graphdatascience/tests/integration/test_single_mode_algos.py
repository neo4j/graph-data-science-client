from typing import Generator

from pytest import fixture

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

GRAPH_NAME = "g"


@fixture(autouse=True)
def run_around_tests(runner: Neo4jQueryRunner) -> Generator[None, None, None]:
    # Runs before each test
    runner.run_query(
        """
        CREATE
        (a: Node),
        (b: Node),
        (c: Node),
        (a)-[:REL]->(b),
        (a)-[:REL]->(c),
        (b)-[:REL]->(c)
        """
    )

    yield  # Test runs here

    # Runs after each test
    runner.run_query("MATCH (n) DETACH DELETE n")
    runner.run_query(f"CALL gds.graph.drop('{GRAPH_NAME}')")


def test_triangles(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", {"REL": {"orientation": "UNDIRECTED"}})

    result = gds.alpha.triangles(G, maxDegree=2)

    assert len(result) == 1
