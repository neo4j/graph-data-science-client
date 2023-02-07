from typing import Generator

import pytest

from graphdatascience import GraphDataScience
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

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


def test_bogus_algo(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")
    with pytest.raises(SyntaxError, match="There is no 'gds.bogusAlgo.stream' to call"):
        gds.bogusAlgo.stream(G)


def test_suggest_correct_endpoint(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    # Similar enough
    with pytest.raises(
        SyntaxError, match="There is no 'gds.pagerank.stream' to call. Did you mean 'gds.pageRank.stream'?"
    ):
        gds.pagerank.stream(G)

    # Too different
    with pytest.raises(SyntaxError, match="There is no 'gds.peggyRanker.stream' to call$"):
        gds.peggyRanker.stream(G)


def test_no_custom_cypher_exception(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")
    with pytest.raises(
        Exception,
        match="There is no procedure with the name `gds.pagerank.stream` registered for this database instance",
    ):
        gds.run_cypher(f"CALL gds.pagerank.stream('{G.name()}')")
