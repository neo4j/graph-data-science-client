from pathlib import Path
from typing import Generator

import pytest
from testcontainers.core.network import Network

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from tests.integration.conftest import (
    create_db_query_runner,
    start_database,
)


@pytest.fixture(scope="package")
def neo4j_connection(
    network: Network, logs_dir: Path, request: pytest.FixtureRequest
) -> Generator[DbmsConnectionInfo, None, None]:
    yield from start_database(logs_dir, network, request)


@pytest.fixture(scope="package")
def db_query_runner(neo4j_connection: DbmsConnectionInfo) -> Generator[Neo4jQueryRunner, None, None]:
    yield from create_db_query_runner(neo4j_connection)
