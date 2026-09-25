from pathlib import Path
from typing import Generator

import pytest
from testcontainers.core.network import Network

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from tests.integration.services import (
    GdsSessionConnectionInfo,
    create_arrow_client,
    no_runtime_session_alias,
    runtime_session_alias,
    start_session,
)

ignore_preview_warning = pytest.mark.filterwarnings("ignore:.*is a preview feature:UserWarning")


@pytest.fixture(scope="package")
def session_connection_runtime(
    network: Network,
    logs_dir: Path,
    models_dir: Path,
    runtime_api: str,
    gds_api_connection: str,
    request: pytest.FixtureRequest,
) -> Generator[GdsSessionConnectionInfo, None, None]:
    yield from start_session(
        logs_dir,
        models_dir,
        network,
        request.node.name,
        gds_api_uri=gds_api_connection,
        runtime_api_uri=runtime_api,
        session_alias=runtime_session_alias(),
    )


@pytest.fixture(scope="package")
def arrow_client_runtime(session_connection_runtime: GdsSessionConnectionInfo) -> AuthenticatedArrowClient:
    """Arrow client backed by a session wired to the python-runtime API (needed by FastPath).

    Package-scoped on purpose: the runtime-backed session and its mock runtime API are
    stopped again once this package's tests are done, instead of idling until the end of
    the whole test session.
    """
    return create_arrow_client(session_connection_runtime)


@pytest.fixture(scope="package")
def session_connection_no_runtime(
    network: Network,
    logs_dir: Path,
    models_dir: Path,
    gds_api_connection: str,
    request: pytest.FixtureRequest,
) -> Generator[GdsSessionConnectionInfo, None, None]:
    yield from start_session(
        logs_dir,
        models_dir,
        network,
        request.node.name,
        gds_api_uri=gds_api_connection,
        session_alias=no_runtime_session_alias(),
    )


@pytest.fixture(scope="package")
def arrow_client_no_runtime(session_connection_no_runtime: GdsSessionConnectionInfo) -> AuthenticatedArrowClient:
    """Arrow client backed by a session WITHOUT the python-runtime API.

    The shared session is runtime-enabled, so python-runtime backed endpoints like FastPath
    are only expected to fail on a session started without the runtime API.
    """
    return create_arrow_client(session_connection_no_runtime)
