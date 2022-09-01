import re

import pytest
from neo4j import Driver

from graphdatascience.error.unable_to_connect import UnableToConnectError
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.tests.integration.conftest import AUTH, URI
from graphdatascience.version import __version__

GRAPH_NAME = "g"


@pytest.mark.skip_on_aura
def test_switching_db(runner: Neo4jQueryRunner) -> None:
    default_database = runner.database()
    runner.run_query("CREATE (a: Node)")

    pre_count = runner.run_query("MATCH (n: Node) RETURN COUNT(n) AS c")["c"][0]
    assert pre_count == 1

    MY_DB_NAME = "my-db"
    runner.run_query("CREATE DATABASE $dbName", {"dbName": MY_DB_NAME})
    runner.set_database(MY_DB_NAME)

    post_count = runner.run_query("MATCH (n: Node) RETURN COUNT(n) AS c")["c"][0]
    assert post_count == 0

    runner.set_database(default_database)  # type: ignore
    runner.run_query("MATCH (n) DETACH DELETE n")
    runner.run_query("DROP DATABASE $dbName", {"dbName": MY_DB_NAME})


@pytest.mark.skip_on_aura
def test_run_query_with_db(runner: Neo4jQueryRunner) -> None:
    runner.run_query("CREATE (a: Node)")

    default_db_count = runner.run_query("MATCH (n: Node) RETURN COUNT(n) AS c")["c"][0]
    assert default_db_count == 1

    MY_DB_NAME = "my-db"
    runner.run_query("CREATE DATABASE $dbName", {"dbName": MY_DB_NAME})
    # runner.set_database(MY_DB_NAME)

    specified_db_count = runner.run_query("MATCH (n: Node) RETURN COUNT(n) AS c", db=MY_DB_NAME)["c"][0]
    assert specified_db_count == 0

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
    with pytest.raises(UnableToConnectError):
        GraphDataScience("neo4j+s://localhost:7687", auth=AUTH, aura_ds=True)


def test_run_cypher(gds: GraphDataScience) -> None:
    result = gds.run_cypher("CALL gds.list()")
    assert len(result) > 10


def test_server_version(gds: GraphDataScience) -> None:
    cached_server_version = gds._server_version
    server_version_string = gds.version()

    server_version_match = re.search(r"^(\d+)\.(\d+)\.(\d+)", server_version_string)
    assert server_version_match

    server_version = server_version_match.groups()

    assert cached_server_version.major == int(server_version[0])
    assert cached_server_version.minor == int(server_version[1])
    assert cached_server_version.patch == int(server_version[2])


def test_no_db_explicitly_set() -> None:
    gds = GraphDataScience(URI, AUTH)
    result = gds.run_cypher("CALL gds.list()")
    assert len(result) > 10
