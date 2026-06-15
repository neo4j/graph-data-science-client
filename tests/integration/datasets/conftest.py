from pathlib import Path
from typing import Generator

import pytest
from testcontainers.core.network import Network

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from tests.integration.conftest import GdsSessionConnectionInfo, create_arrow_client, start_session


@pytest.fixture(scope="package")
def session_connection(
    network: Network,
    tmp_path_factory: pytest.TempPathFactory,
    logs_dir: Path,
    request: pytest.FixtureRequest,
) -> Generator[GdsSessionConnectionInfo, None, None]:
    yield from start_session(logs_dir, tmp_path_factory, network, request)  # type: ignore[arg-type]


@pytest.fixture(scope="package")
def arrow_client(session_connection: GdsSessionConnectionInfo) -> AuthenticatedArrowClient:
    return create_arrow_client(session_connection)
