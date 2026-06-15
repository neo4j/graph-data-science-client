from typing import Generator

import pytest
from testcontainers.neo4j import Neo4jContainer

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from tests.integration.conftest import create_plugin_query_runner


@pytest.fixture(scope="package")
def query_runner(gds_plugin_container: Neo4jContainer) -> Generator[Neo4jQueryRunner, None, None]:
    yield from create_plugin_query_runner(gds_plugin_container)
