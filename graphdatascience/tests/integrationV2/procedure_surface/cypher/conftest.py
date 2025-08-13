import os
from pathlib import Path
from typing import Generator

import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.neo4j import Neo4jContainer

from graphdatascience import QueryRunner
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


@pytest.fixture(scope="package")
def gds_plugin_container(logs_dir: Path, inside_ci: bool) -> Generator[Neo4jContainer, None, None]:
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
        stdout, stderr = neo4j_db.get_logs()
        if stderr:
            print(f"Error logs from Neo4j container:\n{stderr}")

        if inside_ci:
            print(f"Neo4j container logs:\n{stdout}")

        out_file = logs_dir / "neo4j_container.log"
        with open(out_file, "w") as f:
            f.write(str(stdout))


@pytest.fixture(scope="package")
def query_runner(gds_plugin_container: DockerContainer) -> Generator[QueryRunner, None, None]:
    host = gds_plugin_container.get_container_host_ip()

    query_runner = Neo4jQueryRunner.create_for_db(
        f"bolt://{host}:{gds_plugin_container.get_exposed_port(7687)}",
        ("neo4j", "password"),
    )
    yield query_runner
    query_runner.close()
