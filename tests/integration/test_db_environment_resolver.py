import pytest

from graphdatascience.query_runner.db_environment_resolver import DbEnvironmentResolver
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


@pytest.mark.only_on_aura
def test_hosted_in_aura_aura_dbms(runner: Neo4jQueryRunner) -> None:
    assert DbEnvironmentResolver.hosted_in_aura(runner)


@pytest.mark.skip_on_aura
def test_hosted_in_aura_self_managed_dbms(runner: Neo4jQueryRunner) -> None:
    assert not DbEnvironmentResolver.hosted_in_aura(runner)
