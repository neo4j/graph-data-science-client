from neo4j import Driver
from pandas import DataFrame

from graphdatascience import ServerVersion
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.query_runner.session_query_runner import SessionQueryRunner
from graphdatascience.tests.unit.conftest import CollectingQueryRunner
from graphdatascience.tests.unit.test_session_query_runner import FakeArrowClient


def test_disabled_progress_logging(neo4j_driver: Driver):
    query_runner = Neo4jQueryRunner.create(neo4j_driver, show_progress=False)
    assert query_runner._resolve_show_progress(True) is False
    assert query_runner._resolve_show_progress(False) is False


def test_enabled_progress_logging(neo4j_driver: Driver):
    query_runner = Neo4jQueryRunner.create(neo4j_driver, show_progress=True)
    assert query_runner._resolve_show_progress(True) is True
    assert query_runner._resolve_show_progress(False) is False


def test_disabled_progress_logging_session(neo4j_driver: Driver):
    version = ServerVersion(2, 7, 0)
    db_query_runner = CollectingQueryRunner(version, result_mock=DataFrame([{"version": "v1"}]))
    gds_query_runner = CollectingQueryRunner(version)
    query_runner = SessionQueryRunner.create(
        gds_query_runner,
        db_query_runner,
        FakeArrowClient(),  # type: ignore
        show_progress=False,
    )
    assert query_runner._resolve_show_progress(True) is False
    assert query_runner._resolve_show_progress(False) is False


def test_enabled_progress_logging_session(neo4j_driver: Driver):
    version = ServerVersion(2, 7, 0)
    db_query_runner = CollectingQueryRunner(version, result_mock=DataFrame([{"version": "v1"}]))
    gds_query_runner = CollectingQueryRunner(version)
    query_runner = SessionQueryRunner.create(
        gds_query_runner,
        db_query_runner,
        FakeArrowClient(),  # type: ignore
        show_progress=True,
    )
    assert query_runner._resolve_show_progress(True) is True
    assert query_runner._resolve_show_progress(False) is False
