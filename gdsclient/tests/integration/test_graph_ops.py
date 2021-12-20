from neo4j import GraphDatabase
from pytest import fixture

from gdsclient import GraphDataScience, Neo4jQueryRunner


URI = "bolt://localhost:7687"
GRAPH_NAME = "g"
DRIVER = GraphDatabase.driver(URI)
RUNNER = Neo4jQueryRunner(DRIVER)
gds = GraphDataScience(RUNNER)


@fixture(autouse=True)
def run_around_tests():
    # Runs before each test
    with DRIVER.session() as session:
        session.run(
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
    with DRIVER.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        session.run(f"CALL gds.graph.drop('{GRAPH_NAME}')")


def test_create_graph_native():
    graph = gds.graph.create(GRAPH_NAME, "*", "*")
    assert graph

    result = RUNNER.run_query(f"CALL gds.graph.exists('{GRAPH_NAME}') YIELD exists")
    assert result[0]["exists"]


def test_create_graph_native_estimate():
    result = gds.graph.create.estimate("*", "*")

    assert result[0]["requiredMemory"]
