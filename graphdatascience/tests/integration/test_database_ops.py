import pytest
from neo4j import DEFAULT_DATABASE, Driver

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.tests.integration.conftest import AUTH, URI
from graphdatascience.version import __version__

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


def test_from_neo4j_driver(neo4j_driver: Driver) -> None:
    gds = GraphDataScience.from_neo4j_driver(neo4j_driver)
    assert len(gds.list()) > 10


def test_from_neo4j_credentials() -> None:
    gds = GraphDataScience(URI, auth=AUTH)
    assert len(gds.list()) > 10
    assert gds.driver_config()["user_agent"] == f"neo4j-graphdatascience-v{__version__}"


def test_aurads_rejects_bolt() -> None:
    with pytest.raises(
        ValueError,
        match=r"AuraDS requires using the 'neo4j\+s' protocol \('bolt' was provided\)",
    ):
        GraphDataScience("bolt://localhost:7687", auth=AUTH, aura_ds=True)


def test_aurads_rejects_neo4j() -> None:
    with pytest.raises(
        ValueError,
        match=r"AuraDS requires using the 'neo4j\+s' protocol \('neo4j' was provided\)",
    ):
        GraphDataScience("neo4j://localhost:7687", auth=AUTH, aura_ds=True)


def test_aurads_rejects_neo4j_ssc() -> None:
    with pytest.raises(
        ValueError,
        match=r"AuraDS requires using the 'neo4j\+s' protocol \('neo4j\+ssc' was provided\)",
    ):
        GraphDataScience("neo4j+ssc://localhost:7687", auth=AUTH, aura_ds=True)


def test_aurads_accepts_neo4j_s() -> None:
    gds = GraphDataScience("neo4j+s://localhost:7687", auth=AUTH, aura_ds=True)

    assert gds.driver_config()["keep_alive"]
    assert gds.driver_config()["max_connection_lifetime"] == 60 * 8
    assert gds.driver_config()["max_connection_pool_size"] == 50
    assert gds.driver_config()["user_agent"] == f"neo4j-graphdatascience-v{__version__}"


def test_run_cypher(gds: GraphDataScience) -> None:
    result = gds.run_cypher("CALL gds.list()")
    assert len(result) > 10
