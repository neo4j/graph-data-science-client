import logging
import os
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator, Optional

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.docker_client import DockerClient
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import HttpWaitStrategy

from graphdatascience.arrow_client.arrow_authentication import UsernamePasswordAuthentication
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient

LOGGER = logging.getLogger(__name__)


def pytest_collection_modifyitems(config: Any, items: Any) -> None:
    if not config.getoption("--include-ogb"):
        skip_ogb = pytest.mark.skip(reason="need --include-ogb option to run")
        for item in items:
            if "ogb" in item.keywords:
                item.add_marker(skip_ogb)


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


def _current_container_id() -> Optional[str]:
    """Detect: are we running inside a docker container that the sibling docker
    daemon knows about? Returns its id, or None for host runs.

    TeamCity is expected to set TEST_CONTAINER_ID explicitly (e.g. via
    `--cidfile` or a fixed `--name`); we also try `socket.gethostname()` as a
    fallback because docker sets the short container id as the hostname by
    default. Errors are logged (not swallowed silently) so a misconfigured CI
    environment fails loudly rather than hanging.
    """
    candidate = os.environ.get("TEST_CONTAINER_ID") or socket.gethostname()
    print(f"[v2-it] resolving self container id via candidate={candidate!r}", flush=True)
    if not candidate:
        return None
    try:
        container = DockerClient().client.containers.get(candidate)
    except Exception as e:
        print(f"[v2-it] daemon could not find container {candidate!r}: {e}", flush=True)
        return None
    print(f"[v2-it] resolved current container id: {container.id}", flush=True)
    return str(container.id)


@pytest.fixture(scope="package")
def network() -> Generator[Network, None, None]:
    with Network() as network:
        self_id = _current_container_id()
        if self_id is not None:
            print(f"[v2-it] attaching {self_id[:12]} to test network {network.name}", flush=True)
            network.connect(self_id)
        elif inside_ci():
            raise RuntimeError(
                "Running inside CI (BUILD_NUMBER is set) but could not determine "
                "this process's docker container id; the test container must be "
                "attachable to the testcontainers network. Set TEST_CONTAINER_ID "
                "in the build step or run the test container with a `--name` that "
                "matches its hostname."
            )
        try:
            yield network
        finally:
            # Detach ourselves so the testcontainers `Network.remove()` on
            # context exit isn't blocked by an "active endpoints" error.
            if self_id is not None:
                try:
                    network._unwrap_network.disconnect(self_id)
                    print(f"[v2-it] detached {self_id[:12]} from test network {network.name}", flush=True)
                except Exception as e:
                    print(f"[v2-it] failed to detach {self_id[:12]} from test network: {e}", flush=True)


def start_session(
    logs_dir: Path, tmp_path_factory: pytest.TempPathFactory, network: Network, request: pytest.FixtureRequest
) -> Generator[GdsSessionConnectionInfo, None, None]:
    if (session_uri := os.environ.get("GDS_SESSION_URI")) is not None:
        uri_parts = session_uri.split(":")
        yield GdsSessionConnectionInfo(host=uri_parts[0], arrow_port=8491, bolt_port=int(uri_parts[1]))
        return

    session_image = os.getenv(
        "GDS_SESSION_IMAGE", "europe-west1-docker.pkg.dev/gds-aura-artefacts/gds/gds-session:latest"
    )
    LOGGER.info(f"Using session image: {session_image}")

    model_dir = tmp_path_factory.mktemp("models")
    model_dir.chmod(0o777)  # allow other user inside container to write to model dir

    session_container = (
        DockerContainer(
            image=session_image,
        )
        .with_env("ALLOW_LIST", "DEFAULT")
        .with_env("DNS_NAME", "gds-session")
        .with_env("PAGE_CACHE_SIZE", "100M")
        .with_env("MODEL_STORAGE_BASE_LOCATION", "/models")
        .with_env("ENVIRONMENT", "local")
        .with_env("SESSION_ID", 42)
        .with_env("EXTRA_FLAGS", "--disable-authentication")
        .with_volume_mapping(model_dir, "/models", mode="rw")
        .with_exposed_ports(8491, 8080)
        .waiting_for(HttpWaitStrategy(8080, path="/available"))
    )
    session_container = session_container.with_network(network).with_network_aliases("gds-session")
    with session_container as session_container:
        try:
            # When the test process itself is attached to the test network (CI),
            # reach the session by its alias + internal port. Otherwise we are
            # on the docker host and must use the exposed host port.
            if _current_container_id() is not None:
                host, arrow_port = "gds-session", 8491
            else:
                host, arrow_port = (
                    session_container.get_container_host_ip(),
                    int(session_container.get_exposed_port(8491)),
                )
            print(f"[v2-it] session reachable at {host}:{arrow_port}", flush=True)
            yield GdsSessionConnectionInfo(
                host=host,
                arrow_port=arrow_port,
                bolt_port=-1,  # not used in tests
            )
        finally:
            stdout, stderr = session_container.get_logs()
            stderr_lines = stderr.decode("utf-8", errors="replace").splitlines()
            stderr_lines = [line for line in stderr_lines if line.strip() and "log4j" not in line.lower()]

            if stderr_lines:
                log_lines = "\n".join(stderr_lines)
                LOGGER.info(f"Error logs from session container:\n{log_lines}")

            if inside_ci():
                print(f"Session container logs:\n{stdout}")

            session_logs_dir = logs_dir / request.node.name
            session_logs_dir.mkdir(parents=True, exist_ok=True)
            session_logs_dir.chmod(0o777)

            out_file = session_logs_dir / "session_container.log"
            with open(out_file, "w") as f:
                f.write(stdout.decode("utf-8"))


def create_arrow_client(session_uri: GdsSessionConnectionInfo) -> AuthenticatedArrowClient:
    """Create an authenticated Arrow client connected to the session container."""

    return AuthenticatedArrowClient(
        (session_uri.host, session_uri.arrow_port),
        auth=UsernamePasswordAuthentication("neo4j", "password"),
        encrypted=False,
        advertised_listen_address=("gds-session", 8491),
    )
