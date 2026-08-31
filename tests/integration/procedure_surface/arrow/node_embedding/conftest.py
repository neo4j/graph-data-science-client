from pathlib import Path
from typing import Generator

import pytest
from testcontainers.core.network import Network

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from tests.integration.services import (
    RUNTIME_SESSION_ALIAS,
    GdsSessionConnectionInfo,
    create_arrow_client,
    start_runtime_api,
    start_session,
)


@pytest.fixture(scope="package")
def runtime_api(network: Network, logs_dir: Path, request: pytest.FixtureRequest) -> Generator[str, None, None]:
    yield from start_runtime_api(logs_dir, network, request)


@pytest.fixture(scope="package")
def session_connection_runtime(
    network: Network,
    tmp_path_factory: pytest.TempPathFactory,
    logs_dir: Path,
    runtime_api: str,
    gds_api_connection: str,
    request: pytest.FixtureRequest,
) -> Generator[GdsSessionConnectionInfo, None, None]:
    yield from start_session(
        logs_dir,
        tmp_path_factory,
        network,
        request,
        gds_api_uri=gds_api_connection,
        runtime_api_uri=runtime_api,
        session_alias=RUNTIME_SESSION_ALIAS,
    )


@pytest.fixture(scope="package")
def arrow_client_runtime(session_connection_runtime: GdsSessionConnectionInfo) -> AuthenticatedArrowClient:
    """Arrow client backed by a session wired to the python-runtime API (needed by FastPath).

    Package-scoped on purpose: the runtime-backed session and its mock runtime API are
    stopped again once this package's tests are done, instead of idling until the end of
    the whole test session.
    """
    return create_arrow_client(session_connection_runtime)
