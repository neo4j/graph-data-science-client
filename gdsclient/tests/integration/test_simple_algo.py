from gdsclient import GraphDataScience, Neo4jQueryRunner
from neo4j import GraphDatabase


URI = "bolt://localhost:7687"
GRAPH_NAME = "g"
driver = GraphDatabase.driver(URI)


def setup_module():
    with driver.session() as session:
        session.run("CREATE (:Node)-[:REL]->(:Node)")


def test_pageRank_mutate():
    runner = Neo4jQueryRunner(driver)
    gds = GraphDataScience(runner)
    graph = gds.graph.create(GRAPH_NAME, "Node", "REL")
    gds.pageRank.mutate(graph, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)


def teardown_module():
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        session.run(f"CALL gds.graph.drop('{GRAPH_NAME}')")
