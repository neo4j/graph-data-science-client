from neo4j import DEFAULT_DATABASE

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

GRAPH_NAME = "g"


def test_switching_db(runner: Neo4jQueryRunner) -> None:
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
