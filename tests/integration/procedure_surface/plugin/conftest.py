from typing import Generator

import pytest
from testcontainers.neo4j import Neo4jContainer

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from tests.integration.conftest import create_plugin_query_runner


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
def query_runner(gds_plugin_container: Neo4jContainer) -> Generator[Neo4jQueryRunner, None, None]:
    yield from create_plugin_query_runner(gds_plugin_container)
