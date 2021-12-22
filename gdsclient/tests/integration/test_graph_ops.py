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
        (a: Node {x: 1}),
        (b: Node {x: 2}),
        (c: Node {x: 3}),
        (a)-[:REL]->(b),
        (a)-[:REL]->(c),
        (b)-[:REL]->(c)
        """
    )

    yield  # Test runs here

    # Runs after each test
    runner.run_query("MATCH (n) DETACH DELETE n")
    runner.run_query(f"CALL gds.graph.drop('{GRAPH_NAME}', false)")


def test_project_graph_native():
    graph = gds.graph.project(GRAPH_NAME, "*", "*")
    assert graph.name == GRAPH_NAME

    result = runner.run_query(f"CALL gds.graph.exists('{GRAPH_NAME}') YIELD exists")
    assert result[0]["exists"]


def test_project_graph_native_estimate():
    result = gds.graph.project.estimate("*", "*")

    assert result[0]["requiredMemory"]


def test_project_graph_cypher():
    node_query = "MATCH (n:Node) RETURN id(n) as id"
    relationship_query = (
        "MATCH (n:Node)-->(m:Node) RETURN id(n) as source, id(m) as target, 'T' as type"
    )
    graph = gds.graph.project.cypher(GRAPH_NAME, node_query, relationship_query)
    assert graph.name == GRAPH_NAME

    result = runner.run_query(f"CALL gds.graph.exists('{GRAPH_NAME}') YIELD exists")
    assert result[0]["exists"]


def test_project_graph_cypher_estimate():
    node_query = "MATCH (n:Node) RETURN id(n) as id"
    relationship_query = (
        "MATCH (n:Node)-->(m:Node) RETURN id(n) as source, id(m) as target, 'T' as type"
    )
    result = gds.graph.project.cypher.estimate(node_query, relationship_query)

    assert result[0]["requiredMemory"]


def test_project_subgraph():
    from_graph = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    subgraph = gds.beta.graph.project.subgraph(
        "s", from_graph, "n.x > 1", "*", concurrency=2
    )

    assert subgraph.name == "s"

    runner.run_query(f"CALL gds.graph.drop('{subgraph.name}')")


def test_graph_list():
    result = gds.graph.list()
    assert len(result) == 0

    graph = gds.graph.project(GRAPH_NAME, "*", "*")
    result = gds.graph.list()
    assert len(result) == 1

    result = gds.graph.list(graph)
    assert result[0]["graphName"] == GRAPH_NAME


def teardown_module():
    driver.close()
