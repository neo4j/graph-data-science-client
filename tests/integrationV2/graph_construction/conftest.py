from pathlib import Path
from typing import Generator

import pytest
from testcontainers.neo4j import Neo4jContainer

from graphdatascience.arrow_client.arrow_authentication import UsernamePasswordAuthentication
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v1.gds_arrow_client import GdsArrowClient
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from tests.integrationV2.conftest import GdsSessionConnectionInfo, create_arrow_client, start_session
from tests.integrationV2.procedure_surface.conftest import start_gds_plugin_database

# --- GDS Plugin fixtures (used by CypherGraphConstructor and ArrowV1GraphConstructor tests) ---


@pytest.fixture(scope="package")
def gds_plugin_container(
    logs_dir: Path, tmp_path_factory: pytest.TempPathFactory, request: pytest.FixtureRequest
) -> Generator[Neo4jContainer, None, None]:
    yield from start_gds_plugin_database(logs_dir, tmp_path_factory, request)


@pytest.fixture(scope="package")
def query_runner(gds_plugin_container: Neo4jContainer) -> Generator[Neo4jQueryRunner, None, None]:
    host = gds_plugin_container.get_container_host_ip()
    port = gds_plugin_container.get_exposed_port(7687)

    runner = Neo4jQueryRunner.create_for_db(
        f"bolt://{host}:{port}",
        ("neo4j", "password"),
    )
    runner.set_database("neo4j")
    yield runner
    runner.close()


@pytest.fixture(scope="package")
def gds_arrow_client(gds_plugin_container: Neo4jContainer) -> Generator[GdsArrowClient, None, None]:
    arrow_port = int(gds_plugin_container.get_exposed_port(8491))
    with GdsArrowClient(
        flight_client=AuthenticatedArrowClient(
            (gds_plugin_container.get_container_host_ip(), arrow_port),
            auth=UsernamePasswordAuthentication("neo4j", "password"),
            encrypted=False,
        )
    ) as client:
        yield client


# --- GDS Session fixtures (used by ArrowV2GraphConstructor tests) ---


@pytest.fixture(scope="package")
def session_connection(
    network: object,
    tmp_path_factory: pytest.TempPathFactory,
    logs_dir: Path,
    request: pytest.FixtureRequest,
) -> Generator[GdsSessionConnectionInfo, None, None]:
    yield from start_session(logs_dir, tmp_path_factory, network, request)  # type: ignore[arg-type]


@pytest.fixture(scope="package")
def arrow_client(session_connection: GdsSessionConnectionInfo) -> AuthenticatedArrowClient:
    return create_arrow_client(session_connection)
