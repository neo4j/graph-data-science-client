import logging
import os
from pathlib import Path
from typing import Generator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.arrow_authentication import UsernamePasswordAuthentication
from graphdatascience.arrow_client.arrow_info import ArrowInfo
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="package")
def password_dir(tmpdir_factory: pytest.TempdirFactory) -> Generator[Path, None, None]:
    """Create a temporary file and return its path."""
    tmp_dir = tmpdir_factory.mktemp("passwords")
    temp_file_path = os.path.join(tmp_dir, "password")

    with open(temp_file_path, "w") as f:
        f.write("password")

    yield tmp_dir

    # Clean up the file
    os.unlink(temp_file_path)

@pytest.fixture(scope="package")
def network() -> Generator[Network, None, None]:
    with Network() as network:
        yield network

@pytest.fixture(scope="package")
def session_container(network: Network, password_dir: Path, logs_dir: Path, inside_ci: bool) -> Generator[DockerContainer, None, None]:
    session_image = os.getenv(
        "GDS_SESSION_IMAGE", "europe-west1-docker.pkg.dev/gds-aura-artefacts/gds/gds-session:latest"
    )

    LOGGER.info(f"Using session image: {session_image}")

    session_container = (
        DockerContainer(
            image=session_image,
        )
        .with_env("ALLOW_LIST", "DEFAULT")
        .with_env("DNS_NAME", "gds-session")
        .with_env("PAGE_CACHE_SIZE", "100M")
        .with_exposed_ports(8491)
        .with_network_aliases("gds-session")
        .with_volume_mapping(password_dir, "/passwords")
        .with_kwargs(extra_hosts=["host.docker.internal:host-gateway"])
        # .with_network(network)
    )

    with session_container as session_container:
        wait_for_logs(session_container, "Running GDS tasks: 0")
        yield session_container
        stdout, stderr = session_container.get_logs()

        if stderr:
            print(f"Error logs from session container:\n{stderr}")

        if inside_ci:
            print(f"Session container logs:\n{stdout}")

        out_file = logs_dir / "session_container.log"
        with open(out_file, "w") as f:
            f.write(stdout.decode("utf-8"))


@pytest.fixture(scope="package")
def arrow_client(session_container: DockerContainer) -> AuthenticatedArrowClient:
    """Create an authenticated Arrow client connected to the session container."""
    host = "host.docker.internal"
    port = session_container.get_exposed_port(8491)

    return AuthenticatedArrowClient.create(
        arrow_info=ArrowInfo(f"{host}:{port}", True, True, ["v1", "v2"]),
        auth=UsernamePasswordAuthentication("neo4j", "password"),
        encrypted=False,
        advertised_listen_address=("gds-session", 8491),
    )


@pytest.fixture(scope="package")
def neo4j_container(password_file: str, network: Network) -> Generator[DockerContainer, None, None]:
    neo4j_image = os.getenv("NEO4J_DATABASE_IMAGE")

    if neo4j_image is None:
        raise ValueError("NEO4J_DATABASE_IMAGE environment variable is not set")

    db_container = (
        DockerContainer(image=neo4j_image)
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        .with_env("NEO4J_AUTH", "neo4j/password")
        .with_env("NEO4J_server_jvm_additional", "-Dcom.neo4j.arrow.GdsFeatureToggles.enableGds=false")
        .with_env("NEO4J_server_bolt_advertised__address", "host.docker.internal:7687")
        .with_network_aliases("neo4j-db")
        .with_network(network)
        .with_bind_ports(7687, 7687)
        .with_kwargs(extra_hosts=["host.docker.internal:host-gateway"])
    )

    with db_container as db_container:
        wait_for_logs(db_container, "Started.")
        yield db_container
        # stdout, stderr = db_container.get_logs()
        # print(stdout)


@pytest.fixture(scope="package")
def query_runner(neo4j_container: DockerContainer) -> Generator[QueryRunner, None, None]:
    host = "host.docker.internal"
    port = 7687

    query_runner = Neo4jQueryRunner.create_for_db(
        f"bolt://{host}:{port}",
        ("neo4j", "password"),
    )
    yield query_runner
    query_runner.close()
