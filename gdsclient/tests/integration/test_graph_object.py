import pytest
from neo4j import GraphDatabase

from gdsclient import GraphDataScience, Neo4jQueryRunner

from . import AUTH, URI

GRAPH_NAME = "g"


def setup_module():
    global driver
    global G
    global runner
    global gds

    driver = GraphDatabase.driver(URI, auth=AUTH)
    runner = Neo4jQueryRunner(driver)

    runner.run_query(
        """
        CREATE
        (a: Node {x: 1}),
        (b: Node {x: 2}),
        (c: Node {x: 3}),
        (a)-[:REL {y: 42.0}]->(b),
        (a)-[:REL {y: 13.37}]->(c),
        (b)-[:REL {z: 7.9}]->(c)
        """
    )

    gds = GraphDataScience(runner)
    G = project_graph()


def project_graph():
    return gds.graph.project(
        GRAPH_NAME, {"Node": {"properties": "x"}}, {"REL": {"properties": ["y", "z"]}}
    )


def test_graph_node_count():
    assert G.node_count() == 3


def test_graph_relationship_count():
    assert G.relationship_count() == 3


def test_graph_node_properties():
    assert G.node_properties("Node") == ["x"]


def test_graph_relationship_properties():
    assert G.relationship_properties("REL") == ["y", "z"]


def test_graph_degree_distribution():
    assert G.degree_distribution()["mean"] == 2.0


def test_graph_density():
    assert G.density() == 0.5


def test_graph_memory_usage():
    assert G.memory_usage()


def test_graph_size_in_bytes():
    assert G.size_in_bytes() > 0


def test_graph_exists():
    global G

    assert G.exists()

    gds.graph.drop(G)

    result = gds.graph.exists(G.name())
    assert not result[0]["exists"]

    G = project_graph()


def test_graph_drop():
    global G

    result = gds.graph.exists(G.name())
    assert result[0]["exists"]
    G.node_count()

    G.drop()

    result = gds.graph.exists(G.name())
    assert not result[0]["exists"]

    with pytest.raises(ValueError):
        G.node_count()

    G = project_graph()


def teardown_module():
    runner.run_query("MATCH (n) DETACH DELETE n")
    runner.run_query(f"CALL gds.graph.drop('{GRAPH_NAME}', false)")
    driver.close()
