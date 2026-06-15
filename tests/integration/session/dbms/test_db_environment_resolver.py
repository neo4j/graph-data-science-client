import pytest

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session.dbms.db_environment_resolver import DbEnvironmentResolver


def test_hosted_in_aura_aura_dbms(db_query_runner: Neo4jQueryRunner) -> None:
    assert not DbEnvironmentResolver.hosted_in_aura(db_query_runner)
