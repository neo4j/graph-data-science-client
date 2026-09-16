from unittest.mock import Mock

from pandas import DataFrame
from pytest_mock import MockerFixture

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


def test_resolve_hosted_in_aura_sets_field_and_returns_true(mocker: MockerFixture) -> None:
    db_runner = Neo4jQueryRunner(driver=Mock(), protocol="bolt")
    mocker.patch.object(db_runner, "run_retryable_cypher", return_value=DataFrame({"hostedInAura": [True]}))

    assert db_runner.resolve_hosted_in_aura() is True
    assert db_runner.hosted_in_aura is True


def test_resolve_hosted_in_aura_sets_field_and_returns_false(mocker: MockerFixture) -> None:
    db_runner = Neo4jQueryRunner(driver=Mock(), protocol="bolt")
    mocker.patch.object(db_runner, "run_retryable_cypher", return_value=DataFrame({"hostedInAura": [False]}))

    assert db_runner.resolve_hosted_in_aura() is False
    assert db_runner.hosted_in_aura is False
