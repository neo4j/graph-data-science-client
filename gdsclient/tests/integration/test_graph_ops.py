from neo4j import GraphDatabase
from pytest import fixture

from gdsclient import GraphDataScience, Neo4jQueryRunner

URI = "bolt://localhost:7687"
GRAPH_NAME = "g"
driver = GraphDatabase.driver(URI)
runner = Neo4jQueryRunner(driver)
gds = GraphDataScience(runner)


@fixture(autouse=True)
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


def test_create_graph_native():
    graph = gds.graph.create(GRAPH_NAME, "*", "*")
    assert graph

    result = runner.run_query(f"CALL gds.graph.exists('{GRAPH_NAME}') YIELD exists")
    assert result[0]["exists"]


def test_create_graph_native_estimate():
    result = gds.graph.create.estimate("*", "*")

    assert result[0]["requiredMemory"]


def test_create_graph_cypher():
    node_query = "MATCH (n:Node) RETURN id(n) as id"
    relationship_query = (
        "MATCH (n:Node)-->(m:Node) RETURN id(n) as source, id(m) as target, 'T' as type"
    )
    graph = gds.graph.create.cypher(GRAPH_NAME, node_query, relationship_query)
    assert graph

    result = runner.run_query(f"CALL gds.graph.exists('{GRAPH_NAME}') YIELD exists")
    assert result[0]["exists"]


def test_create_graph_cypher_estimate():
    node_query = "MATCH (n:Node) RETURN id(n) as id"
    relationship_query = (
        "MATCH (n:Node)-->(m:Node) RETURN id(n) as source, id(m) as target, 'T' as type"
    )
    result = gds.graph.create.cypher.estimate(node_query, relationship_query)

    assert result[0]["requiredMemory"]


def teardown_module():
    driver.close()
