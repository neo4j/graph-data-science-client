from typing import Generator

import pytest

from gdsclient import GraphDataScience, Neo4jQueryRunner

GRAPH_NAME = "g"


@pytest.fixture(autouse=True)
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
    runner.run_query(f"CALL gds.graph.drop('{GRAPH_NAME}', false)")


def test_nonexisting_algo(gds: GraphDataScience) -> None:
    G = gds.graph.project(GRAPH_NAME, "*", "*")
    with pytest.raises(Exception):
        gds.bogusAlgo.stream(G)
