import os
import tempfile
from typing import Generator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.neo4j import Neo4jContainer

from graphdatascience.arrow_client.arrow_authentication import UsernamePasswordAuthentication
from graphdatascience.arrow_client.arrow_info import ArrowInfo
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient


@pytest.fixture(scope="session")
def password_file() -> Generator[str, None, None]:
    """Create a temporary file and return its path."""
    temp_dir = tempfile.mkdtemp()
    temp_file_path = os.path.join(temp_dir, "password")

    with open(temp_file_path, "w") as f:
        f.write("password")

    yield temp_dir

    # Clean up the file and directory
    os.unlink(temp_file_path)
    os.rmdir(temp_dir)


@pytest.fixture(scope="session")
def neo4j_database_container() -> Generator[Neo4jContainer, None, None]:
    neo4j_image = os.getenv("NEO4J_DATABASE_IMAGE", "neo4j:5.11-enterprise")

    neo4j_container = (
        Neo4jContainer(
            image=neo4j_image,
        )
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        .with_env("NEO4J_dbms_security_procedures_unrestricted", "gds.*")
        .with_env("NEO4J_dbms_security_procedures_allowlist", "gds.*")
    )

    with neo4j_container as neo4j_db:
        wait_for_logs(neo4j_db)
        yield neo4j_db


@pytest.fixture(scope="session")
def session_container(password_file: str) -> Generator[DockerContainer, None, None]:
    session_image = os.getenv("GDS_SESSION_IMAGE")

    if session_image is None:
        raise ValueError("GDS_SESSION_IMAGE environment variable is not set")

    session_container = (
        DockerContainer(
            image=session_image,
        )
        .with_env("ALLOW_LIST", "DEFAULT")
        .with_env("DNS_NAME", "gds-session")
        .with_env("PAGE_CACHE_SIZE", "100M")
        .with_exposed_ports(8491)
        .with_network_aliases(["gds-session"])
        .with_volume_mapping(password_file, "/passwords")
    )

    with session_container as session_container:
        wait_for_logs(session_container, "Running GDS tasks: 0")
        yield session_container
        stdout, stderr = session_container.get_logs()
        print(stdout)


@pytest.fixture
def arrow_client(session_container: DockerContainer) -> AuthenticatedArrowClient:
    """Create an authenticated Arrow client connected to the session container."""
    host = session_container.get_container_host_ip()
    port = session_container.get_exposed_port(8491)

    return AuthenticatedArrowClient.create(
        arrow_info=ArrowInfo(f"{host}:{port}", True, True, ["v1", "v2"]),
        auth=UsernamePasswordAuthentication("neo4j", "password"),
        encrypted=False,
    )
