import os
import tempfile
from typing import Generator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.arrow_authentication import UsernamePasswordAuthentication
from graphdatascience.arrow_client.arrow_info import ArrowInfo
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


@pytest.fixture(scope="package")
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


@pytest.fixture(scope="package")
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


@pytest.fixture(scope="package")
def arrow_client(session_container: DockerContainer) -> AuthenticatedArrowClient:
    """Create an authenticated Arrow client connected to the session container."""
    host = session_container.get_container_host_ip()
    port = session_container.get_exposed_port(8491)

    return AuthenticatedArrowClient.create(
        arrow_info=ArrowInfo(f"{host}:{port}", True, True, ["v1", "v2"]),
        auth=UsernamePasswordAuthentication("neo4j", "password"),
        encrypted=False,
    )


@pytest.fixture(scope="package")
def neo4j_container(password_file: str) -> Generator[DockerContainer, None, None]:
    neo4j_image = os.getenv("NEO4J_DATABASE_IMAGE")

    if neo4j_image is None:
        raise ValueError("NEO4J_DATABASE_IMAGE environment variable is not set")

    db_container = (
        DockerContainer(image=neo4j_image)
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        .with_env("NEO4J_AUTH", "neo4j/password")
        .with_env("NEO4J_server_jvm_additional", "-Dcom.neo4j.arrow.GdsFeatureToggles.enableGds=false")
    )

    if os.getenv("BUILD_NUMBER") is None:
        db_container.with_kwargs(network_mode="host")
    else:
        db_container.with_exposed_ports(7687)

    with db_container as db_container:
        wait_for_logs(db_container, "Started.")
        yield db_container
        # stdout, stderr = db_container.get_logs()
        # print(stdout)


@pytest.fixture(scope="package")
def query_runner(neo4j_container: DockerContainer) -> Generator[QueryRunner, None, None]:
    host = "localhost"
    port = 7687
    if os.getenv("BUILD_NUMBER") is not None:
        host = neo4j_container.get_container_host_ip()
        port = neo4j_container.get_exposed_port(7687)

    query_runner = Neo4jQueryRunner.create_for_db(
        f"bolt://{host}:{port}",
        ("neo4j", "password"),
    )
    yield query_runner
    query_runner.close()
