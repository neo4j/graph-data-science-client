import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Generator

import pytest
from dateutil.relativedelta import relativedelta
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs

from graphdatascience.arrow_client.arrow_authentication import UsernamePasswordAuthentication
from graphdatascience.arrow_client.arrow_info import ArrowInfo
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="package")
def password_dir(tmp_path_factory: pytest.TempPathFactory) -> Generator[Path, None, None]:
    """Create a temporary file and return its path."""
    tmp_dir = tmp_path_factory.mktemp("passwords")
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


def latest_neo4j_version() -> str:
    today = datetime.now()
    previous_month = today - relativedelta(months=1)
    return previous_month.strftime("%Y.%m.0")


def start_session(
    inside_ci: bool, logs_dir: Path, network: Network, password_dir: Path
) -> Generator[DockerContainer, None, None]:
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
        .with_volume_mapping(password_dir, "/passwords")
    )
    if not inside_ci:
        session_container = session_container.with_network(network).with_network_aliases("gds-session")
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


def create_arrow_client(session_container: DockerContainer) -> AuthenticatedArrowClient:
    """Create an authenticated Arrow client connected to the session container."""
    host = session_container.get_container_host_ip()
    port = session_container.get_exposed_port(8491)
    return AuthenticatedArrowClient.create(
        arrow_info=ArrowInfo(f"{host}:{port}", True, True, ["v1", "v2"]),
        auth=UsernamePasswordAuthentication("neo4j", "password"),
        encrypted=False,
        advertised_listen_address=("gds-session", 8491),
    )


def start_database(inside_ci: bool, logs_dir: Path, network: Network) -> Generator[DockerContainer, None, None]:
    default_neo4j_image = (
        f"europe-west1-docker.pkg.dev/neo4j-aura-image-artifacts/aura/neo4j-enterprise:{latest_neo4j_version()}"
    )
    neo4j_image = os.getenv("NEO4J_DATABASE_IMAGE", default_neo4j_image)
    if neo4j_image is None:
        raise ValueError("NEO4J_DATABASE_IMAGE environment variable is not set")
    db_logs_dir = logs_dir / "arrow_surface" / "db_logs"
    db_logs_dir.mkdir(parents=True)
    db_logs_dir.chmod(0o777)
    db_container = (
        DockerContainer(image=neo4j_image)
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        .with_env("NEO4J_AUTH", "neo4j/password")
        .with_env("NEO4J_server_jvm_additional", "-Dcom.neo4j.arrow.GdsFeatureToggles.enableGds=false")
        .with_env("NEO4J_server_bolt_advertised__address", "localhost:7687")
        .with_network_aliases("neo4j-db")
        .with_network(network)
        .with_bind_ports(7687, 7687)
        .with_volume_mapping(db_logs_dir, "/logs", mode="rw")
    )
    with db_container as db_container:
        wait_for_logs(db_container, "Started.")
        yield db_container
        stdout, stderr = db_container.get_logs()

        if stderr:
            print(f"Error logs from database container:\n{stderr}")

        if inside_ci:
            print(f"Database container logs:\n{stdout}")

        out_file = db_logs_dir / "stdout.log"
        with open(out_file, "w") as f:
            f.write(stdout.decode("utf-8"))


def create_db_query_runner(neo4j_container: DockerContainer) -> Generator[Neo4jQueryRunner, None, None]:
    host = neo4j_container.get_container_host_ip()
    port = 7687
    query_runner = Neo4jQueryRunner.create_for_db(
        f"bolt://{host}:{port}",
        ("neo4j", "password"),
    )
    yield query_runner
    query_runner.close()
