import pytest

from graphdatascience.query_runner.standalone_session_query_runner import StandaloneSessionQueryRunner
from graphdatascience.tests.unit.conftest import CollectingQueryRunner


def test_disallow_database_operations(runner: CollectingQueryRunner) -> None:
    query_runner = StandaloneSessionQueryRunner(runner)

    with pytest.raises(NotImplementedError):
        query_runner.run_cypher("MATCH something")

    with pytest.raises(NotImplementedError):
        query_runner.set_database("neo4j")

    with pytest.raises(NotImplementedError):
        query_runner.database()

    with pytest.raises(NotImplementedError):
        query_runner.bookmarks()

    with pytest.raises(NotImplementedError):
        query_runner.set_bookmarks(None)

    with pytest.raises(NotImplementedError):
        query_runner.last_bookmarks()

    with pytest.raises(NotImplementedError):
        query_runner.driver_config()


def test_disallow_write_procedures(runner: CollectingQueryRunner) -> None:
    query_runner = StandaloneSessionQueryRunner(runner)

    with pytest.raises(NotImplementedError, match="write procedures are not supported on standalone sessions"):
        query_runner.call_procedure("gds.graph.write", None)
