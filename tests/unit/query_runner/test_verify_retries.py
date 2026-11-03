from __future__ import annotations

from unittest.mock import Mock

import neo4j
import pytest

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


@pytest.fixture
def driver() -> Mock:
    return Mock()


@pytest.fixture
def runner(driver: Mock) -> Neo4jQueryRunner:
    return Neo4jQueryRunner(driver=driver, protocol="bolt")


def _pool_acquisition_timeout_error() -> Exception:
    # neo4j 6+ raises a dedicated class; earlier versions a plain ClientError
    # carrying the same message
    message = "failed to obtain a connection from the pool within 10.0s (timeout)"
    error_class: type[Exception] | None = getattr(neo4j.exceptions, "ConnectionAcquisitionTimeoutError", None)
    if error_class is not None:
        return error_class(message)
    return neo4j.exceptions.ClientError(message)


def test_verify_connectivity_bounds_acquisition_timeout(runner: Neo4jQueryRunner, driver: Mock) -> None:
    runner.verify_connectivity()

    driver.verify_connectivity.assert_called_once_with(
        connection_acquisition_timeout=Neo4jQueryRunner._VERIFY_CONNECTION_ACQUISITION_TIMEOUT
    )


def test_verify_authentication_bounds_acquisition_timeout(runner: Neo4jQueryRunner, driver: Mock) -> None:
    runner.verify_authentication()

    driver.verify_authentication.assert_called_once_with(
        connection_acquisition_timeout=Neo4jQueryRunner._VERIFY_CONNECTION_ACQUISITION_TIMEOUT
    )


def test_verify_connectivity_retries_pool_acquisition_timeout(
    runner: Neo4jQueryRunner, driver: Mock, monkeypatch: pytest.MonkeyPatch
) -> None:
    # skip the retry sleep
    monkeypatch.setattr(
        Neo4jQueryRunner.verify_connectivity.retry,  # type: ignore[attr-defined]
        "sleep",
        lambda _: None,
    )
    driver.verify_connectivity.side_effect = [_pool_acquisition_timeout_error(), None]

    runner.verify_connectivity()

    assert driver.verify_connectivity.call_count == 2
