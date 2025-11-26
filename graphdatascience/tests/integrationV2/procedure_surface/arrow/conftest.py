import logging
from pathlib import Path
from typing import Generator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.tests.integrationV2.conftest import (
    GdsSessionConnectionInfo,
    create_arrow_client,
    inside_ci,
    start_session,
)
from graphdatascience.tests.integrationV2.procedure_surface.conftest import (
    create_db_query_runner,
    start_database,
)

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="package")
def session_connection(
    network: Network, password_dir: Path, logs_dir: Path
) -> Generator[GdsSessionConnectionInfo, None, None]:
    yield from start_session(logs_dir, network, password_dir)


@pytest.fixture(scope="package")
def arrow_client(session_connection: DockerContainer) -> AuthenticatedArrowClient:
    return create_arrow_client(session_connection)


@pytest.fixture(scope="package")
def neo4j_connection(network: Network, logs_dir: Path) -> Generator[DbmsConnectionInfo, None, None]:
    if inside_ci():
        raise RuntimeError("Communication between Session and DB is not supported yet in CI.")
    yield from start_database(logs_dir, network)


@pytest.fixture(scope="package")
def query_runner(neo4j_connection: DbmsConnectionInfo) -> Generator[QueryRunner, None, None]:
    yield from create_db_query_runner(neo4j_connection)
