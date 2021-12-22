from neo4j import DEFAULT_DATABASE, GraphDatabase
from pytest import fixture

from gdsclient import GraphDataScience, Neo4jQueryRunner

URI = "bolt://localhost:7687"
GRAPH_NAME = "g"
driver = GraphDatabase.driver(URI)
runner = Neo4jQueryRunner(driver)
gds = GraphDataScience(runner)

runner.run_query(
    """
    CREATE
    (a: Node {x: 1}),
    (b: Node {x: 2}),
    (c: Node {x: 3}),
    (a)-[:REL]->(b),
    (a)-[:REL]->(c),
    (b)-[:REL]->(c)
    """
)
graph = gds.graph.project(GRAPH_NAME, "*", "*")


def test_graph_node_count():
    assert graph.node_count() == 3


def test_graph_relationship_count():
    assert graph.relationship_count() == 3


def teardown_module():
    runner.run_query("MATCH (n) DETACH DELETE n")
    runner.run_query(f"CALL gds.graph.drop('{GRAPH_NAME}', false)")
    driver.close()
