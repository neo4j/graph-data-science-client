import logging
import os
import socket
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Optional

import dotenv
import pytest
from dateutil.relativedelta import relativedelta
from testcontainers.core.container import DockerContainer
from testcontainers.core.docker_client import DockerClient
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import HttpWaitStrategy, LogMessageWaitStrategy
from testcontainers.neo4j import Neo4jContainer

from graphdatascience.arrow_client.arrow_authentication import UsernamePasswordAuthentication
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v1.gds_arrow_client import GdsArrowClient
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo

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
    return os.environ.get("BUILD_ID") is not None


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
                "Running inside CI (BUILD_ID is set) but could not determine "
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


# --------------------------------------------------------------------------- #
# GDS Session (Arrow-only) container
# --------------------------------------------------------------------------- #


def start_session(
    logs_dir: Path, tmp_path_factory: pytest.TempPathFactory, network: Network, request: pytest.FixtureRequest
) -> Generator[GdsSessionConnectionInfo, None, None]:
    if (session_uri := os.environ.get("GDS_SESSION_URI")) is not None:
        uri_parts = session_uri.split(":")
        yield GdsSessionConnectionInfo(host=uri_parts[0], arrow_port=8491, bolt_port=int(uri_parts[1]))
        return

    session_image = os.getenv(
        "GDS_SESSION_IMAGE", "europe-west1-docker.pkg.dev/gds-aura-artefacts/gds/gds-session:aura-release"
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


@pytest.fixture(scope="package")
def session_connection(
    network: Network,
    tmp_path_factory: pytest.TempPathFactory,
    logs_dir: Path,
    request: pytest.FixtureRequest,
) -> Generator[GdsSessionConnectionInfo, None, None]:
    yield from start_session(logs_dir, tmp_path_factory, network, request)


@pytest.fixture(scope="package")
def arrow_client(session_connection: GdsSessionConnectionInfo) -> AuthenticatedArrowClient:
    return create_arrow_client(session_connection)


# --------------------------------------------------------------------------- #
# Neo4j database (no GDS) container
# --------------------------------------------------------------------------- #


def latest_neo4j_version() -> str:
    today = datetime.now()

    previous_month = today - relativedelta(months=1)

    overrides = {"2025.12.0": "2025.12.1-1", "2026.01.0": "2026.01.2"}

    cal_ver = previous_month.strftime("%Y.%m.0")

    return overrides.get(cal_ver, cal_ver)


def start_database(
    logs_dir: Path, network: Network, request: pytest.FixtureRequest
) -> Generator[DbmsConnectionInfo, None, None]:
    default_neo4j_image = (
        f"europe-west1-docker.pkg.dev/neo4j-aura-image-artifacts/aura-dev/neo4j-enterprise:{latest_neo4j_version()}"
    )
    neo4j_image = os.getenv("NEO4J_DATABASE_IMAGE", default_neo4j_image)
    if neo4j_image is None:
        raise ValueError("NEO4J_DATABASE_IMAGE environment variable is not set")

    advertise_address = "neo4j-db" if inside_ci() else "localhost"

    db_logs_dir = logs_dir / request.node.name / "db_logs"
    db_logs_dir.mkdir(parents=True, exist_ok=True)
    db_logs_dir.chmod(0o777)
    db_container = (
        DockerContainer(image=neo4j_image)
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        .with_env("NEO4J_AUTH", "neo4j/password")
        .with_env("NEO4J_server_jvm_additional", "-Dcom.neo4j.arrow.GdsFeatureToggles.enableGds=false")
        .with_env("NEO4J_server_bolt_advertised__address", f"{advertise_address}:7687")
        .with_network_aliases("neo4j-db")
        .with_network(network)
        .with_bind_ports(7687, 7687)
        .with_volume_mapping(db_logs_dir, "/logs", mode="rw")
        .waiting_for(LogMessageWaitStrategy("Started."))
    )
    with db_container as db_container:
        try:
            if _current_container_id() is not None:
                uri = "neo4j-db:7687"
            else:
                uri = f"{db_container.get_container_host_ip()}:{db_container.get_exposed_port(7687)}"
            print(f"[v2-it] neo4j reachable at {uri}", flush=True)
            yield DbmsConnectionInfo(
                uri=uri,
                username="neo4j",
                password="password",
            )
        finally:
            stdout, stderr = db_container.get_logs()

            if stderr:
                print(f"Error logs from database container:\n{stderr}")

            if inside_ci():
                print(f"Database container logs:\n{stdout}")

            out_file = db_logs_dir / "stdout.log"
            with open(out_file, "w") as f:
                f.write(stdout.decode("utf-8"))


def create_db_query_runner(neo4j_connection: DbmsConnectionInfo) -> Generator[Neo4jQueryRunner, None, None]:
    query_runner = Neo4jQueryRunner.create_for_db(
        f"bolt://{neo4j_connection.uri}",
        ("neo4j", "password"),
    )
    query_runner.set_database("neo4j")
    yield query_runner
    query_runner.close()


# --------------------------------------------------------------------------- #
# Neo4j + GDS plugin container
# --------------------------------------------------------------------------- #


def start_gds_plugin_database(
    logs_dir: Path, tmp_path_factory: pytest.TempPathFactory, request: pytest.FixtureRequest
) -> Generator[Neo4jContainer, None, None]:
    neo4j_image = os.getenv("NEO4J_DATABASE_IMAGE", "neo4j:enterprise")

    dotenv.load_dotenv(Path(__file__).parent.parent / "test.env", override=True)
    GDS_LICENSE_KEY = os.getenv("GDS_LICENSE_KEY")

    if GDS_LICENSE_KEY is None:
        raise ValueError("Trying to start a Plugin database, but no GDS_LICENSE_KEY environment variable was set")

    db_logs_dir = logs_dir / request.node.name / "db_logs"
    db_logs_dir.mkdir(parents=True)
    db_logs_dir.chmod(0o777)

    models_dir = tmp_path_factory.mktemp("models")
    models_dir.chmod(0o777)

    neo4j_container = (
        Neo4jContainer(
            image=neo4j_image,
        )
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        .with_env("NEO4J_PLUGINS", '["graph-data-science"]')
        .with_env("NEO4J_gds_arrow_enabled", "true")
        .with_env("NEO4J_gds_arrow_listen__address", "0.0.0.0:8491")
        .with_env("NEO4J_gds_model_store__location", "/models")
        .with_exposed_ports(8491)
        .with_volume_mapping(db_logs_dir, "/logs", mode="rw")
        .with_volume_mapping(models_dir, "/models", mode="rw")
        .waiting_for(LogMessageWaitStrategy("Started."))
    )

    license_dir = tmp_path_factory.mktemp("gds_license")
    license_dir.chmod(0o755)
    license_file = os.path.join(license_dir, "license_key")
    with open(license_file, "w") as f:
        f.write(GDS_LICENSE_KEY)

    neo4j_container.with_volume_mapping(
        license_dir,
        "/licenses",
    )
    neo4j_container.with_env("NEO4J_gds_enterprise_license__file", "/licenses/license_key")

    with neo4j_container as neo4j_db:
        try:
            yield neo4j_db
        finally:
            stdout, stderr = neo4j_db.get_logs()
            if stderr:
                print(f"Error logs from Neo4j container:\n{stderr}")

            if inside_ci():
                print(f"Neo4j container logs:\n{stdout}")

            out_file = db_logs_dir / "stdout.log"
            with open(out_file, "w") as f:
                f.write(stdout.decode("utf-8"))


def create_plugin_query_runner(container: Neo4jContainer) -> Generator[Neo4jQueryRunner, None, None]:
    """Create a query runner connected to the bolt endpoint of a GDS plugin container."""
    host = container.get_container_host_ip()
    port = container.get_exposed_port(7687)

    query_runner = Neo4jQueryRunner.create_for_db(
        f"bolt://{host}:{port}",
        ("neo4j", "password"),
    )
    query_runner.set_database("neo4j")
    yield query_runner
    query_runner.close()


def create_gds_arrow_client(container: Neo4jContainer) -> Generator[GdsArrowClient, None, None]:
    """Create a v1 Arrow client connected to the arrow endpoint of a GDS plugin container."""
    arrow_port = int(container.get_exposed_port(8491))
    with GdsArrowClient(
        flight_client=AuthenticatedArrowClient(
            (container.get_container_host_ip(), arrow_port),
            auth=UsernamePasswordAuthentication("neo4j", "password"),
            encrypted=False,
        )
    ) as client:
        yield client


@pytest.fixture(scope="package")
def gds_plugin_container(
    logs_dir: Path, tmp_path_factory: pytest.TempPathFactory, request: pytest.FixtureRequest
) -> Generator[Neo4jContainer, None, None]:
    yield from start_gds_plugin_database(logs_dir, tmp_path_factory, request)


@pytest.fixture(scope="package")
def gds_arrow_client(gds_plugin_container: Neo4jContainer) -> Generator[GdsArrowClient, None, None]:
    yield from create_gds_arrow_client(gds_plugin_container)
