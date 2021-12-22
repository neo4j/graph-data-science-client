import pytest
from neo4j import GraphDatabase

from gdsclient import GraphDataScience, Neo4jQueryRunner

URI = "bolt://localhost:7687"
GRAPH_NAME = "g"
driver = GraphDatabase.driver(URI)
runner = Neo4jQueryRunner(driver)
gds = GraphDataScience(runner)


@pytest.fixture(autouse=True)
def run_around_tests():
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


def test_nonexisting_algo():
    graph = gds.graph.project(GRAPH_NAME, "*", "*")
    with pytest.raises(Exception):
        gds.bogusAlgo.stream(graph)


def teardown_module():
    driver.close()
