import asyncio
import re
import time

import pytest
from neo4j import Driver

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.query_runner.progress.static_progress_provider import StaticProgressProvider
from graphdatascience.tests.integration.conftest import AUTH, URI
from graphdatascience.version import __version__

GRAPH_NAME = "g"


@pytest.mark.skip_on_aura
def test_init_without_neo4j_db(runner: Neo4jQueryRunner) -> None:
    default_database = runner.database()

    MY_DB_NAME = "bananas"
    runner.run_cypher("CREATE DATABASE $dbName WAIT", {"dbName": MY_DB_NAME}, database=default_database)

    runner.run_cypher("DROP DATABASE $dbName WAIT", {"dbName": default_database}, database=MY_DB_NAME)

    try:
        gds = GraphDataScience(URI, AUTH, database=MY_DB_NAME)
        gds.close()
    finally:
        runner.run_cypher("CREATE DATABASE $dbName WAIT", {"dbName": default_database}, database=MY_DB_NAME)
        runner.run_cypher("DROP DATABASE $dbName WAIT", {"dbName": MY_DB_NAME}, database=default_database)


@pytest.mark.skip_on_aura
def test_switching_db(runner: Neo4jQueryRunner) -> None:
    default_database = runner.database()
    runner.run_cypher("CREATE (a: Node)")

    pre_count = runner.run_cypher("MATCH (n: Node) RETURN COUNT(n) AS c").squeeze()
    assert pre_count == 1

    MY_DB_NAME = "my-db"
    runner.run_cypher("CREATE DATABASE $dbName WAIT", {"dbName": MY_DB_NAME})
    try:
        runner.set_database(MY_DB_NAME)
        post_count = runner.run_cypher("MATCH (n) RETURN COUNT(n) AS c").squeeze()
        assert post_count == 0
    finally:
        runner.set_database(default_database)  # type: ignore
        runner.run_cypher("MATCH (n) DETACH DELETE n")
        runner.run_cypher("DROP DATABASE $dbName WAIT", {"dbName": MY_DB_NAME})


@pytest.mark.skip_on_aura
def test_switching_db_and_use_graph(gds: GraphDataScience) -> None:
    default_database = gds.database()
    gds.run_cypher("CREATE (a: A)")

    G_A, _ = gds.graph.project(GRAPH_NAME, "A", "*")

    assert G_A.node_count() == 1

    MY_DB_NAME = "my-db"
    gds.run_cypher("CREATE DATABASE $dbName WAIT", {"dbName": MY_DB_NAME})
    gds.set_database(MY_DB_NAME)

    try:
        gds.run_cypher("CREATE (b1: B), (b2: B)")
        G_B, _ = gds.graph.project(GRAPH_NAME, "B", "*")

        assert G_B.node_count() == 2
    finally:
        gds.set_database(default_database)  # type: ignore
        gds.run_cypher("MATCH (n) DETACH DELETE n")
        gds.run_cypher("DROP DATABASE $dbName WAIT", {"dbName": MY_DB_NAME})


@pytest.mark.skip_on_aura
def test_run_query_with_db(runner: Neo4jQueryRunner) -> None:
    runner.run_cypher("CREATE (a: Node)")

    default_db_count = runner.run_cypher("MATCH (n: Node) RETURN COUNT(n) AS c").squeeze()
    assert default_db_count == 1

    MY_DB_NAME = "my-db"
    runner.run_cypher("CREATE DATABASE $dbName WAIT", {"dbName": MY_DB_NAME})

    try:
        specified_db_count = runner.run_cypher("MATCH (n) RETURN COUNT(n) AS c", database=MY_DB_NAME).squeeze()
        assert specified_db_count == 0
    finally:
        runner.run_cypher("MATCH (n) DETACH DELETE n")
        runner.run_cypher("DROP DATABASE $dbName WAIT", {"dbName": MY_DB_NAME})


@pytest.mark.skip_on_aura
def test_initialize_with_db(runner: Neo4jQueryRunner) -> None:
    runner.run_cypher("CREATE (a: Node)")

    default_db_count = runner.run_cypher("MATCH (n: Node) RETURN COUNT(n) AS c").squeeze()
    assert default_db_count == 1

    MY_DB_NAME = "my-db"
    runner.run_cypher("CREATE DATABASE $dbName WAIT", {"dbName": MY_DB_NAME})

    gds_with_specified_db = GraphDataScience(URI, AUTH, database=MY_DB_NAME)

    try:
        specified_db_count = gds_with_specified_db.run_cypher(
            "MATCH (n) RETURN COUNT(n) AS c", database=MY_DB_NAME
        ).squeeze()
        assert specified_db_count == 0
    finally:
        runner.run_cypher("MATCH (n) DETACH DELETE n")
        runner.run_cypher("DROP DATABASE $dbName WAIT", {"dbName": MY_DB_NAME})
        gds_with_specified_db.close()


def test_from_neo4j_driver(neo4j_driver: Driver) -> None:
    gds = GraphDataScience.from_neo4j_driver(neo4j_driver)
    assert len(gds.list()) > 10


def test_from_neo4j_credentials() -> None:
    gds = GraphDataScience(URI, auth=AUTH)
    assert len(gds.list()) > 10
    assert gds.driver_config()["user_agent"] == f"neo4j-graphdatascience-v{__version__}"
    gds.close()


def test_aurads_rejects_bolt() -> None:
    with pytest.raises(
        ValueError,
        match=r"AuraDS requires using the 'neo4j\+s' protocol \('bolt' was provided\)",
    ):
        GraphDataScience._validate_endpoint("bolt://localhost:7687")


def test_aurads_rejects_neo4j() -> None:
    with pytest.raises(
        ValueError,
        match=r"AuraDS requires using the 'neo4j\+s' protocol \('neo4j' was provided\)",
    ):
        GraphDataScience._validate_endpoint("neo4j://localhost:7687")


def test_aurads_rejects_neo4j_ssc() -> None:
    with pytest.raises(
        ValueError,
        match=r"AuraDS requires using the 'neo4j\+s' protocol \('neo4j\+ssc' was provided\)",
    ):
        GraphDataScience._validate_endpoint("neo4j+ssc://localhost:7687")


def test_aurads_accepts_neo4j_s() -> None:
    GraphDataScience._validate_endpoint("neo4j+s://localhost:7687")


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
    gds.close()


def test_warning_when_logging_fails(runner: Neo4jQueryRunner) -> None:
    loop = asyncio.new_event_loop()
    try:

        async def sleep_one() -> None:
            return time.sleep(2)

        task = loop.create_task(sleep_one())

        with pytest.warns(RuntimeWarning, match=r"^Unable to get progress:"):
            loop.run_until_complete(
                runner._progress_logger._log(task, "DUMMY", StaticProgressProvider(), "bad_database")
            )
    finally:
        loop.close()


def test_bookmarks(runner: Neo4jQueryRunner) -> None:
    runner.set_bookmarks(None)
    assert runner.bookmarks() is None

    _ = runner.run_cypher("CREATE (a: Node)")
    assert runner.last_bookmarks() is not None

    runner.set_bookmarks(runner.last_bookmarks())
    assert runner.bookmarks() == runner.last_bookmarks()

    _ = runner.run_cypher("CREATE (b: Node)")
    assert runner.bookmarks() != runner.last_bookmarks()

    runner.run_cypher("MATCH (n) DETACH DELETE n")
    runner.set_bookmarks(None)
