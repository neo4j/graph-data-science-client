"""Fixtures for the CLI job-runner integration tests.

Two session flavours share these fixtures, both built directly against the
containers (bypassing the Aura session-creation path, exactly as
``procedure_surface/session/test_walking_skeleton.py`` does):

* ``gds`` - the plain session (GDS's Java-based algorithms), used by
  ``test_java_based_jobs.py``.
* ``gds_runtime`` - a session wired to the python-runtime API (needed by
  Python-based algorithms like FastPath), used by ``test_python_based_jobs.py``.

Both reuse the shared, session-scoped Neo4j + GDS-session containers from
``tests/integration/conftest.py``.
"""

from pathlib import Path
from typing import Generator
from unittest import mock

import pytest
from gds_cli.common.env import DatabaseConfig
from testcontainers.core.network import Network

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.query_runner import QueryType
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.session.session_lifecycle_manager import SessionLifecycleManager
from tests.integration.conftest import (
    GdsSessionConnectionInfo,
    create_arrow_client,
    create_db_query_runner,
    start_runtime_api,
    start_session,
)

# Distinct alias so the runtime-backed session never collides with the plain
# "gds-session" on the shared network.
RUNTIME_SESSION_ALIAS = "gds-session-with-runtime"


@pytest.fixture(scope="package")
def db_query_runner(neo4j_connection: DbmsConnectionInfo) -> Generator[Neo4jQueryRunner, None, None]:
    yield from create_db_query_runner(neo4j_connection)


@pytest.fixture(scope="package")
def db_config(neo4j_connection: DbmsConnectionInfo) -> DatabaseConfig:
    """Connection details for the CLI's raw-driver uploader, pointing at the same DB."""
    return DatabaseConfig(
        uri=f"bolt://{neo4j_connection.uri}",
        username="neo4j",
        password="password",
        database="neo4j",
    )


@pytest.fixture(scope="package")
def gds(arrow_client: AuthenticatedArrowClient, db_query_runner: Neo4jQueryRunner) -> AuraGraphDataScience:
    """Plain session (Java-based GDS algorithms), wired to the Neo4j container."""
    return AuraGraphDataScience(
        arrow_client,
        db_query_runner,
        session_lifecycle_manager=mock.Mock(spec=SessionLifecycleManager),
    )


# --------------------------------------------------------------------------- #
# python-runtime-backed session (for Python-based algorithms, e.g. FastPath)
# --------------------------------------------------------------------------- #


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
        runtime_api_uri=runtime_api,
        gds_api_uri=gds_api_connection,
        session_alias=RUNTIME_SESSION_ALIAS,
    )


@pytest.fixture(scope="package")
def arrow_client_runtime(session_connection_runtime: GdsSessionConnectionInfo) -> AuthenticatedArrowClient:
    return create_arrow_client(session_connection_runtime)


@pytest.fixture(scope="package")
def gds_runtime(
    arrow_client_runtime: AuthenticatedArrowClient, db_query_runner: Neo4jQueryRunner
) -> AuraGraphDataScience:
    """Session wired to the python-runtime API, wired to the same Neo4j container."""
    return AuraGraphDataScience(
        arrow_client_runtime,
        db_query_runner,
        session_lifecycle_manager=mock.Mock(spec=SessionLifecycleManager),
    )


@pytest.fixture(autouse=True)
def clean_state(db_query_runner: Neo4jQueryRunner) -> Generator[None, None, None]:
    """Wipe the shared Neo4j database around every test.

    Depends only on ``db_query_runner`` (not a specific session) so it works for
    both the plain and runtime test files without spinning up the other session.
    The Neo4j container is session-scoped and shared with the rest of the suite,
    so we clean before *and* after each test: before, so a crash in a prior test
    can't poison this one; after, so the graphs we upload don't leak into other
    packages' tests. In-session catalog graphs are dropped by ``run_all`` itself
    (it projects with ``overwrite_graph`` and drops each graph when done).
    """

    def _wipe() -> None:
        db_query_runner.run_cypher("MATCH (n) DETACH DELETE n", query_type=QueryType.USER_ACTION)

    _wipe()
    yield
    _wipe()
