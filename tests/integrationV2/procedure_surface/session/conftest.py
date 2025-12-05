from pathlib import Path
from typing import Generator

import pytest
from testcontainers.core.network import Network

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from tests.integrationV2.conftest import GdsSessionConnectionInfo, create_arrow_client, start_session
from tests.integrationV2.procedure_surface.conftest import (
    create_db_query_runner,
    start_database,
)


@pytest.fixture(scope="package")
def session_connection(
    network: Network, password_dir: Path, logs_dir: Path
) -> Generator[GdsSessionConnectionInfo, None, None]:
    yield from start_session(logs_dir, network, password_dir)


@pytest.fixture(scope="package")
def arrow_client(session_connection: GdsSessionConnectionInfo) -> AuthenticatedArrowClient:
    return create_arrow_client(session_connection)


@pytest.fixture(scope="package")
def neo4j_connection(network: Network, logs_dir: Path) -> Generator[DbmsConnectionInfo, None, None]:
    yield from start_database(logs_dir, network)


@pytest.fixture(scope="package")
def db_query_runner(neo4j_connection: DbmsConnectionInfo) -> Generator[Neo4jQueryRunner, None, None]:
    yield from create_db_query_runner(neo4j_connection)
