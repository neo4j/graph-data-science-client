from neo4j import DEFAULT_DATABASE, GraphDatabase

from gdsclient import Neo4jQueryRunner

URI = "bolt://localhost:7687"
GRAPH_NAME = "g"
driver = GraphDatabase.driver(URI)
runner = Neo4jQueryRunner(driver)


def test_switching_db():
    runner.run_query("CREATE (a: Node)")

    pre_count = runner.run_query("MATCH (n: Node) RETURN COUNT(n) AS c")[0]["c"]
    assert pre_count == 1

    MY_DB_NAME = "my-db"
    runner.run_query("CREATE DATABASE $dbName", {"dbName": MY_DB_NAME})
    runner.set_database(MY_DB_NAME)

    post_count = runner.run_query("MATCH (n: Node) RETURN COUNT(n) AS c")[0]["c"]
    assert post_count == 0

    runner.set_database(DEFAULT_DATABASE)
    runner.run_query("MATCH (n) DETACH DELETE n")
    runner.run_query("DROP DATABASE $dbName", {"dbName": MY_DB_NAME})


def teardown_module():
    driver.close()
