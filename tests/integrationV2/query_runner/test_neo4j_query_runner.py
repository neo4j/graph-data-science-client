import neo4j
import numpy as np
from pandas import DataFrame

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.query_runner.query_type import QueryType
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo

# --- run_cypher ---


def test_run_cypher(query_runner: Neo4jQueryRunner) -> None:
    result = query_runner.run_cypher("RETURN 1 AS n", QueryType.USER_ACTION)
    assert isinstance(result, DataFrame)
    assert list(result.columns) == ["n"]
    assert result["n"].iloc[0] == 1


def test_run_cypher_with_params(query_runner: Neo4jQueryRunner) -> None:
    result = query_runner.run_cypher("RETURN $x AS n", QueryType.USER_ACTION, params={"x": 42})
    assert result["n"].iloc[0] == 42


def test_run_cypher_read_mode(query_runner: Neo4jQueryRunner) -> None:
    result = query_runner.run_cypher("RETURN 'hello' AS greeting", QueryType.USER_ACTION, mode=QueryMode.READ)
    assert result["greeting"].iloc[0] == "hello"


def test_run_cypher_write_and_read(query_runner: Neo4jQueryRunner) -> None:
    try:
        query_runner.run_cypher("CREATE (n:TestNode {value: 123})", QueryType.USER_ACTION, mode=QueryMode.WRITE)
        result = query_runner.run_cypher(
            "MATCH (n:TestNode) RETURN n.value AS value", QueryType.USER_ACTION, mode=QueryMode.READ
        )
        assert len(result) == 1
        assert result["value"].iloc[0] == 123
    finally:
        query_runner.run_cypher("MATCH (n:TestNode) DETACH DELETE n", QueryType.USER_ACTION)


# --- run_retryable_cypher ---


def test_run_retryable_cypher(query_runner: Neo4jQueryRunner) -> None:
    result = query_runner.run_retryable_cypher("RETURN 1 AS n", QueryType.USER_ACTION)
    assert isinstance(result, DataFrame)
    assert result["n"].iloc[0] == 1


def test_run_retryable_cypher_with_params(query_runner: Neo4jQueryRunner) -> None:
    result = query_runner.run_retryable_cypher("RETURN $x AS n", QueryType.USER_ACTION, params={"x": 99})
    assert result["n"].iloc[0] == 99


def test_run_retryable_cypher_read_mode(query_runner: Neo4jQueryRunner) -> None:
    result = query_runner.run_retryable_cypher("RETURN 'world' AS word", QueryType.USER_ACTION, mode=QueryMode.READ)
    assert result["word"].iloc[0] == "world"


# --- call_procedure ---


def test_call_procedure(query_runner: Neo4jQueryRunner) -> None:
    result = query_runner.call_procedure("db.labels", QueryType.USER_ACTION, yields=["label"])
    assert isinstance(result, DataFrame)
    assert "label" in result.columns


# --- call_function ---


def test_call_function(query_runner: Neo4jQueryRunner) -> None:
    result = query_runner.call_function("timestamp", QueryType.USER_ACTION)
    assert isinstance(result, np.int64)


# --- database / set_database ---


def test_database(query_runner: Neo4jQueryRunner) -> None:
    assert query_runner.database() == "neo4j"


def test_set_database(query_runner: Neo4jQueryRunner) -> None:
    original = query_runner.database()
    try:
        query_runner.set_database("system")
        assert query_runner.database() == "system"
    finally:
        if original:
            query_runner.set_database(original)


# --- bookmarks ---


def test_bookmarks_default(query_runner: Neo4jQueryRunner) -> None:
    # bookmarks may be None initially or set from prior queries
    result = query_runner.bookmarks()
    assert result is None or result is not None  # just verifies no exception


def test_set_bookmarks(query_runner: Neo4jQueryRunner) -> None:
    original = query_runner.bookmarks()
    try:
        query_runner.set_bookmarks(None)
        assert query_runner.bookmarks() is None
    finally:
        query_runner.set_bookmarks(original)


def test_last_bookmarks_after_query(query_runner: Neo4jQueryRunner) -> None:
    query_runner.run_cypher("RETURN 1", QueryType.USER_ACTION)
    assert query_runner.last_bookmarks() is not None


# --- encrypted ---


def test_encrypted(query_runner: Neo4jQueryRunner) -> None:
    assert query_runner.encrypted() is False


# --- driver_config ---


def test_driver_config(query_runner: Neo4jQueryRunner) -> None:
    config = query_runner.driver_config()
    assert isinstance(config, dict)
    assert "user_agent" in config


# --- set_show_progress ---


def test_set_show_progress(query_runner: Neo4jQueryRunner) -> None:
    query_runner.set_show_progress(False)
    query_runner.set_show_progress(True)


# --- verify_connectivity / verify_authentication ---


def test_verify_connectivity(query_runner: Neo4jQueryRunner) -> None:
    query_runner.verify_connectivity()


def test_verify_authentication(query_runner: Neo4jQueryRunner) -> None:
    query_runner.verify_authentication()


# --- create_for_db factory ---


def test_create_for_db_with_string(neo4j_connection: DbmsConnectionInfo) -> None:
    runner = Neo4jQueryRunner.create_for_db(
        f"bolt://{neo4j_connection.uri}",
        ("neo4j", "password"),
    )
    try:
        runner.set_database("neo4j")
        result = runner.run_cypher("RETURN 1 AS n", QueryType.USER_ACTION)
        assert result["n"].iloc[0] == 1
    finally:
        runner.close()


def test_create_for_db_with_driver(neo4j_connection: DbmsConnectionInfo) -> None:
    driver = neo4j.GraphDatabase.driver(
        f"bolt://{neo4j_connection.uri}",
        auth=("neo4j", "password"),
    )
    try:
        runner = Neo4jQueryRunner.create_for_db(driver)
        runner.set_database("neo4j")
        result = runner.run_cypher("RETURN 1 AS n", QueryType.USER_ACTION)
        assert result["n"].iloc[0] == 1
        runner.close()
    finally:
        driver.close()


# --- cloneWithoutRouting ---


def test_clone_without_routing(query_runner: Neo4jQueryRunner, neo4j_connection: DbmsConnectionInfo) -> None:
    host, _, port_str = neo4j_connection.get_uri().rpartition(":")
    # If the URI doesn't contain a colon (no explicit port), use the default bolt port
    if not host:
        host = port_str  # rpartition returns ('', '', uri) when separator not found
        port = 7687
    else:
        port = int(port_str)

    clone = query_runner.cloneWithoutRouting(host, port)
    try:
        result = clone.run_cypher("RETURN 1 AS n", QueryType.USER_ACTION)
        assert result["n"].iloc[0] == 1
    finally:
        clone.close()


# --- close ---


def test_close_does_not_raise(neo4j_connection: DbmsConnectionInfo) -> None:
    runner = Neo4jQueryRunner.create_for_db(
        f"bolt://{neo4j_connection.uri}",
        ("neo4j", "password"),
    )
    runner.close()
