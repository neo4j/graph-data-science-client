from pathlib import Path
from typing import Generator

import pytest
from testcontainers.neo4j import Neo4jContainer

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.tests.integrationV2.procedure_surface.conftest import start_gds_plugin_database


@pytest.fixture(scope="package")
def gds_plugin_container(
    logs_dir: Path, tmp_path_factory: pytest.TempPathFactory
) -> Generator[Neo4jContainer, None, None]:
    yield from start_gds_plugin_database(logs_dir, tmp_path_factory)


@pytest.fixture(scope="package")
def neo4j_connection(gds_plugin_container: Neo4jContainer) -> Generator[DbmsConnectionInfo, None, None]:
    host = gds_plugin_container.get_container_host_ip()
    port = gds_plugin_container.get_exposed_port(7687)

    yield DbmsConnectionInfo(
        uri=f"bolt://{host}:{port}",
        username="neo4j",
        password="password",
    )


@pytest.fixture(scope="package")
def query_runner(neo4j_connection: DbmsConnectionInfo) -> Generator[Neo4jQueryRunner, None, None]:
    query_runner = Neo4jQueryRunner.create_for_db(
        neo4j_connection.uri,
        (neo4j_connection.username, neo4j_connection.password),  # type: ignore
    )

    query_runner.set_database("neo4j")

    yield query_runner
    query_runner.close()
