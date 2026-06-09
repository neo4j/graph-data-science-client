from neo4j import Driver

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


def test_disabled_progress_logging(neo4j_driver: Driver) -> None:
    query_runner = Neo4jQueryRunner.create_for_db(neo4j_driver, show_progress=False)
    assert query_runner._resolve_show_progress(True) is False
    assert query_runner._resolve_show_progress(False) is False


def test_enabled_progress_logging(neo4j_driver: Driver) -> None:
    query_runner = Neo4jQueryRunner.create_for_db(neo4j_driver, show_progress=True)
    assert query_runner._resolve_show_progress(True) is True
    assert query_runner._resolve_show_progress(False) is False
