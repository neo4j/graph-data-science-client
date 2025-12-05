from pathlib import Path
from typing import Generator

import pytest
from testcontainers.neo4j import Neo4jContainer

from graphdatascience.arrow_client.arrow_authentication import UsernamePasswordAuthentication
from graphdatascience.arrow_client.arrow_endpoint_version import ArrowEndpointVersion
from graphdatascience.arrow_client.arrow_info import ArrowInfo
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v1.gds_arrow_client import GdsArrowClient
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from tests.integrationV2.procedure_surface.conftest import start_gds_plugin_database


@pytest.fixture(scope="package")
def gds_plugin_container(
    logs_dir: Path, tmp_path_factory: pytest.TempPathFactory
) -> Generator[Neo4jContainer, None, None]:
    yield from start_gds_plugin_database(logs_dir, tmp_path_factory)


@pytest.fixture(scope="package")
def query_runner(gds_plugin_container: Neo4jContainer) -> Generator[Neo4jQueryRunner, None, None]:
    host = gds_plugin_container.get_container_host_ip()
    port = gds_plugin_container.get_exposed_port(7687)

    query_runner = Neo4jQueryRunner.create_for_db(
        f"bolt://{host}:{port}",
        ("neo4j", "password"),
    )

    query_runner.set_database("neo4j")

    yield query_runner
    query_runner.close()


@pytest.fixture(scope="package")
def gds_arrow_client(gds_plugin_container: Neo4jContainer) -> Generator[GdsArrowClient, None, None]:
    arrow_port = int(gds_plugin_container.get_exposed_port(8491))
    with GdsArrowClient(
        flight_client=AuthenticatedArrowClient.create(
            arrow_info=ArrowInfo(
                listenAddress=f"{gds_plugin_container.get_container_host_ip()}:{arrow_port}",
                enabled=True,
                running=True,
                versions=[ArrowEndpointVersion.V2.version()],
            ),
            auth=UsernamePasswordAuthentication("neo4j", "password"),
            encrypted=False,
        )
    ) as client:
        yield client
