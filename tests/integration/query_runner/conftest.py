from typing import Generator

import pytest

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from tests.integration.conftest import create_db_query_runner


@pytest.fixture(scope="package")
def query_runner(neo4j_connection: DbmsConnectionInfo) -> Generator[Neo4jQueryRunner, None, None]:
    yield from create_db_query_runner(neo4j_connection)
