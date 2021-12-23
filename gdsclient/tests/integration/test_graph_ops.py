import pytest
from neo4j import DEFAULT_DATABASE, GraphDatabase

from gdsclient import GraphDataScience, Neo4jQueryRunner

from . import AUTH, URI

GRAPH_NAME = "g"


def setup_module():
    global driver
    global runner
    global gds

    driver = GraphDatabase.driver(URI, auth=AUTH)
    runner = Neo4jQueryRunner(driver)
    gds = GraphDataScience(runner)


@pytest.fixture(autouse=True)
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
    G = gds.graph.project(GRAPH_NAME, "*", "*")
    assert G.name() == GRAPH_NAME

    result = gds.graph.exists(G.name())
    assert result[0]["exists"]


def test_project_graph_native_estimate():
    result = gds.graph.project.estimate("*", "*")

    assert result[0]["requiredMemory"]


def test_project_graph_cypher():
    node_query = "MATCH (n:Node) RETURN id(n) as id"
    relationship_query = (
        "MATCH (n:Node)-->(m:Node) RETURN id(n) as source, id(m) as target, 'T' as type"
    )
    G = gds.graph.project.cypher(GRAPH_NAME, node_query, relationship_query)
    assert G.name() == GRAPH_NAME

    result = gds.graph.exists(G.name())
    assert result[0]["exists"]


def test_project_graph_cypher_estimate():
    node_query = "MATCH (n:Node) RETURN id(n) as id"
    relationship_query = (
        "MATCH (n:Node)-->(m:Node) RETURN id(n) as source, id(m) as target, 'T' as type"
    )
    result = gds.graph.project.cypher.estimate(node_query, relationship_query)

    assert result[0]["requiredMemory"]


def test_project_subgraph():
    from_G = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    subG = gds.beta.graph.project.subgraph("s", from_G, "n.x > 1", "*", concurrency=2)

    assert subG.name() == "s"

    result = gds.graph.list(subG)
    assert result[0]["nodeCount"] == 2

    runner.run_query(f"CALL gds.graph.drop('{subG.name()}')")


def test_graph_list():
    result = gds.graph.list()
    assert len(result) == 0

    G = gds.graph.project(GRAPH_NAME, "*", "*")
    result = gds.graph.list()
    assert len(result) == 1

    result = gds.graph.list(G)
    assert result[0]["graphName"] == GRAPH_NAME


def test_graph_exists():
    G = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.graph.exists(G.name())
    assert result[0]["exists"]

    result = gds.graph.exists("bogusName")
    assert not result[0]["exists"]


def test_graph_drop():
    G = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.graph.drop(G, True)
    assert result[0]["graphName"] == GRAPH_NAME

    with pytest.raises(ValueError):
        gds.graph.drop(G, True)


def test_graph_export():
    G = gds.graph.project(GRAPH_NAME, "*", "*")

    MY_DB_NAME = "test-database"
    result = gds.graph.export(G, dbName=MY_DB_NAME, batchSize=10000)

    assert result[0]["graphName"] == GRAPH_NAME
    assert result[0]["dbName"] == MY_DB_NAME

    runner.run_query("CREATE DATABASE $dbName", {"dbName": MY_DB_NAME})
    runner.set_database(MY_DB_NAME)
    node_count = runner.run_query("MATCH (n) RETURN COUNT(n) AS c")[0]["c"]

    assert node_count == 3

    runner.run_query("DROP DATABASE $dbName", {"dbName": MY_DB_NAME})
    runner.set_database(DEFAULT_DATABASE)


def test_graph_get():
    gds.graph.project(GRAPH_NAME, "*", "*")

    G = gds.graph.get(GRAPH_NAME)
    assert G.name() == GRAPH_NAME

    with pytest.raises(ValueError):
        gds.graph.get("bogusName")


def teardown_module():
    driver.close()
