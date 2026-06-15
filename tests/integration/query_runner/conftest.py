from pathlib import Path
from typing import Generator

import pytest
from testcontainers.core.network import Network

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from tests.integration.procedure_surface.conftest import start_database


@pytest.fixture(scope="package")
def neo4j_connection(
    network: Network, logs_dir: Path, request: pytest.FixtureRequest
) -> Generator[DbmsConnectionInfo, None, None]:
    yield from start_database(logs_dir, network, request)


@pytest.fixture(scope="package")
def query_runner(neo4j_connection: DbmsConnectionInfo) -> Generator[Neo4jQueryRunner, None, None]:
    runner = Neo4jQueryRunner.create_for_db(
        f"bolt://{neo4j_connection.uri}",
        ("neo4j", "password"),
    )
    runner.set_database("neo4j")
    yield runner
    runner.close()
