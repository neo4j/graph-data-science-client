import os
from typing import Generator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.neo4j import Neo4jContainer

from graphdatascience import QueryRunner
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


@pytest.fixture(scope="session")
def neo4j_database_container() -> Generator[Neo4jContainer, None, None]:
    neo4j_image = os.getenv("NEO4J_DATABASE_IMAGE", "neo4j:enterprise")

    neo4j_container = (
        Neo4jContainer(
            image=neo4j_image,
        )
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        .with_env("NEO4J_PLUGINS", '["graph-data-science"]')
    )

    with neo4j_container as neo4j_db:
        wait_for_logs(neo4j_db, "Started.")
        yield neo4j_db


@pytest.fixture
def query_runner(neo4j_database_container: DockerContainer) -> Generator[QueryRunner, None, None]:
    yield Neo4jQueryRunner.create_for_db(
        f"bolt://localhost:{neo4j_database_container.get_exposed_port(7687)}",
        ("neo4j", "password"),
    )
