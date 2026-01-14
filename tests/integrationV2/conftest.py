import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import HttpWaitStrategy

from graphdatascience.arrow_client.arrow_authentication import UsernamePasswordAuthentication
from graphdatascience.arrow_client.arrow_info import ArrowInfo
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient

LOGGER = logging.getLogger(__name__)


def pytest_collection_modifyitems(config: Any, items: Any) -> None:
    if not config.getoption("--include-integration-v2"):
        skip_v2 = pytest.mark.skip(reason="need --include-integration-v2 option to run")
        for item in items:
            # otherwise would also skip lots of other test
            if "integrationV2" in str(item.fspath):
                item.add_marker(skip_v2)

    if inside_ci():
        skip_ci = pytest.mark.skip(reason="Skipping db_integration tests in CI")
        for item in items:
            if "db_integration" in item.keywords:
                item.add_marker(skip_ci)


# best used with pytest --basetemp=tmp/pytest for easy access to logs
@pytest.fixture(scope="session")
def logs_dir(tmp_path_factory: pytest.TempPathFactory) -> Generator[Path, None, None]:
    """Create a temporary file and return its path."""
    tmp_dir = tmp_path_factory.mktemp("logs")

    yield tmp_dir


def inside_ci() -> bool:
    return os.environ.get("BUILD_NUMBER") is not None


@dataclass
class GdsSessionConnectionInfo:
    host: str
    arrow_port: int
    bolt_port: int


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


def start_session(
    logs_dir: Path, network: Network, password_dir: Path
) -> Generator[GdsSessionConnectionInfo, None, None]:
    if (session_uri := os.environ.get("GDS_SESSION_URI")) is not None:
        uri_parts = session_uri.split(":")
        yield GdsSessionConnectionInfo(host=uri_parts[0], arrow_port=8491, bolt_port=int(uri_parts[1]))
        return

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
        .with_env("MODEL_STORAGE_BASE_LOCATION", "/models")
        .with_exposed_ports(8491, 8080)
        .with_volume_mapping(password_dir, "/passwords")
        .waiting_for(HttpWaitStrategy(8080, path="/available"))
    )
    if not inside_ci():
        session_container = session_container.with_network(network).with_network_aliases("gds-session")
    with session_container as session_container:
        try:
            yield GdsSessionConnectionInfo(
                host=session_container.get_container_host_ip(),
                arrow_port=session_container.get_exposed_port(8491),
                bolt_port=-1,  # not used in tests
            )
        finally:
            stdout, stderr = session_container.get_logs()

            if stderr:
                print(f"Error logs from session container:\n{stderr}")

            if inside_ci():
                print(f"Session container logs:\n{stdout}")

            out_file = logs_dir / "session_container.log"
            with open(out_file, "w") as f:
                f.write(stdout.decode("utf-8"))


def create_arrow_client(session_uri: GdsSessionConnectionInfo) -> AuthenticatedArrowClient:
    """Create an authenticated Arrow client connected to the session container."""

    return AuthenticatedArrowClient.create(
        arrow_info=ArrowInfo(f"{session_uri.host}:{session_uri.arrow_port}", True, True, ["v1", "v2"]),
        auth=UsernamePasswordAuthentication("neo4j", "password"),
        encrypted=False,
        advertised_listen_address=("gds-session", 8491),
    )
