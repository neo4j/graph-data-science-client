"""Pytest fixtures for the integration tests.

The Docker services behind the fixtures are defined in `tests/integration/services.py`;
this file wires them up as pytest fixtures and owns their lifecycle: session-scoped
services are shared for the whole run, while the package-scoped runtime fixtures in
`procedure_surface/arrow/node_embedding/conftest.py` stop again once the FastPath tests
are done.
"""

from pathlib import Path
from typing import Any, Generator

import pytest
from testcontainers.core.network import Network
from testcontainers.neo4j import Neo4jContainer

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v1.gds_arrow_client import GdsArrowClient
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from tests.integration.services import (
    GdsSessionConnectionInfo,
    create_arrow_client,
    create_gds_arrow_client,
    current_container_id,
    db_alias,
    inside_ci,
    session_alias,
    start_database,
    start_gds_api,
    start_gds_plugin_database,
    start_session,
)


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


@pytest.fixture(scope="session")
def network() -> Generator[Network, None, None]:
    with Network() as network:
        self_id = current_container_id()
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
# Service fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="session")
def gds_api_connection(network: Network, logs_dir: Path, request: pytest.FixtureRequest) -> Generator[str, None, None]:
    yield from start_gds_api(logs_dir, network, request)


@pytest.fixture(scope="session")
def session_connection(
    network: Network,
    tmp_path_factory: pytest.TempPathFactory,
    logs_dir: Path,
    gds_api_connection: str,
    request: pytest.FixtureRequest,
) -> Generator[GdsSessionConnectionInfo, None, None]:
    yield from start_session(
        logs_dir, tmp_path_factory, network, request, gds_api_uri=gds_api_connection, session_alias=session_alias()
    )


@pytest.fixture(scope="package")
def arrow_client(session_connection: GdsSessionConnectionInfo) -> AuthenticatedArrowClient:
    return create_arrow_client(session_connection)


@pytest.fixture(scope="session")
def neo4j_connection(
    network: Network, logs_dir: Path, request: pytest.FixtureRequest
) -> Generator[DbmsConnectionInfo, None, None]:
    """Shared plain Neo4j (no GDS) database, reused across all packages that need a bare DB.

    Packages that need a Neo4j+GDS-plugin database instead override this fixture (see
    procedure_surface/plugin/conftest.py).
    """
    yield from start_database(logs_dir, network, request, db_alias=db_alias())


@pytest.fixture(scope="session")
def gds_plugin_container(
    logs_dir: Path, tmp_path_factory: pytest.TempPathFactory, request: pytest.FixtureRequest
) -> Generator[Neo4jContainer, None, None]:
    yield from start_gds_plugin_database(logs_dir, tmp_path_factory, request)


@pytest.fixture(scope="package")
def gds_arrow_client(gds_plugin_container: Neo4jContainer) -> Generator[GdsArrowClient, None, None]:
    yield from create_gds_arrow_client(gds_plugin_container)
