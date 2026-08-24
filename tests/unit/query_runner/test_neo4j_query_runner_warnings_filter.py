from __future__ import annotations

import logging
from typing import Generator
from unittest.mock import Mock

import pytest

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


@pytest.fixture
def clean_notifications_logger() -> Generator[logging.Logger, None, None]:
    logger = logging.getLogger("neo4j.notifications")
    original_filters = list(logger.filters)
    original_flag = getattr(Neo4jQueryRunner, "_warnings_filters_configured", False)
    try:
        yield logger
    finally:
        logger.filters = original_filters
        Neo4jQueryRunner._warnings_filters_configured = original_flag


def _make_record(msg: str) -> logging.LogRecord:
    return logging.LogRecord(
        name="neo4j.notifications",
        level=logging.WARNING,
        pathname=__file__,
        lineno=1,
        msg=msg,
        args=(),
        exc_info=None,
    )


def _configure(runner: Neo4jQueryRunner) -> None:
    runner._Neo4jQueryRunner__configure_warnings_filter()  # type: ignore[attr-defined]


def test_unrelated_notifications_are_kept(clean_notifications_logger: logging.Logger) -> None:
    runner = Neo4jQueryRunner(driver=Mock(), protocol="bolt")
    _configure(runner)

    unrelated_record = _make_record("Some unrelated neo4j notification")
    assert clean_notifications_logger.filter(unrelated_record)


def test_configure_warnings_filter_is_idempotent(clean_notifications_logger: logging.Logger) -> None:
    runner = Neo4jQueryRunner(driver=Mock(), protocol="bolt")
    _configure(runner)
    count_after_first = len(clean_notifications_logger.filters)
    _configure(runner)
    count_after_second = len(clean_notifications_logger.filters)
    assert count_after_second == count_after_first


def test_gds_deprecated_field_notification_is_dropped(clean_notifications_logger: logging.Logger) -> None:
    runner = Neo4jQueryRunner(driver=Mock(), protocol="bolt")
    _configure(runner)

    deprecated_record = _make_record(
        "The query used a deprecated field from a procedure 'gds.pageRank.stream' by 'gds.pagerank.stream'"
    )
    assert not clean_notifications_logger.filter(deprecated_record)


def test_gds_deprecated_procedure_notification_is_dropped(
    clean_notifications_logger: logging.Logger,
) -> None:
    runner = Neo4jQueryRunner(driver=Mock(), protocol="bolt")
    _configure(runner)

    deprecated_record = _make_record("The procedure has a deprecated field 'foo' returned by the procedure 'gds.bar'")
    assert not clean_notifications_logger.filter(deprecated_record)
