from pathlib import Path
from typing import Generator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.tests.integrationV2.procedure_surface.conftest import (
    create_arrow_client,
    create_db_query_runner,
    start_database,
    start_session,
)


@pytest.fixture(scope="package")
def session_url(network: Network, password_dir: Path, logs_dir: Path, inside_ci: bool) -> Generator[str, None, None]:
    yield from start_session(inside_ci, logs_dir, network, password_dir)


@pytest.fixture(scope="package")
def arrow_client(session_url: str) -> AuthenticatedArrowClient:
    return create_arrow_client(session_url)


@pytest.fixture(scope="package")
def neo4j_container(network: Network, logs_dir: Path, inside_ci: bool) -> Generator[DockerContainer, None, None]:
    yield from start_database(inside_ci, logs_dir, network)


@pytest.fixture(scope="package")
def db_query_runner(neo4j_container: DockerContainer) -> Generator[QueryRunner, None, None]:
    yield from create_db_query_runner(neo4j_container)
