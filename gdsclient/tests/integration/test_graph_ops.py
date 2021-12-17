from gdsclient import GDS, Neo4jQueryRunner
from neo4j import GraphDatabase


URI = "bolt://localhost:7687"
GRAPH_NAME = "g"
driver = GraphDatabase.driver(URI)


def setup_module():
    with driver.session() as session:
        session.run("CREATE (:Node)-[:REL]->(:Node)")


def test_create_graph_native():
    runner = Neo4jQueryRunner(driver)
    gds = GDS(runner)
    gds.graph.create(GRAPH_NAME, "Node", "REL")
    runner.run_query(f"CALL gds.graph.exists('{GRAPH_NAME}') YIELD exists")


def teardown_module():
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        session.run(f"CALL gds.graph.drop('{GRAPH_NAME}')")
