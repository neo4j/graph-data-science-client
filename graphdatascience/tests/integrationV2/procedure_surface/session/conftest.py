from pathlib import Path
from typing import Generator

import pytest
from docker.models.networks import Network
from testcontainers.core.container import DockerContainer

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.tests.integrationV2.procedure_surface.conftest import start_session, create_arrow_client, \
    start_database


@pytest.fixture(scope="package")
def session_container(
    network: Network, password_dir: Path, logs_dir: Path, inside_ci: bool
) -> Generator[DockerContainer, None, None]:
    yield from start_session(inside_ci, logs_dir, network, password_dir)


@pytest.fixture(scope="package")
def arrow_client(session_container: DockerContainer) -> AuthenticatedArrowClient:
    return create_arrow_client(session_container)

@pytest.fixture(scope="package")
def neo4j_container(network: Network, logs_dir: Path, inside_ci: bool) -> Generator[DockerContainer, None, None]:
    yield from start_database(inside_ci, logs_dir, network)

