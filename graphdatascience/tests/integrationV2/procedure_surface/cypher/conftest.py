import os
from pathlib import Path
from typing import Generator

import dotenv
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.neo4j import Neo4jContainer

from graphdatascience import QueryRunner
from graphdatascience.query_runner.gds_arrow_client import GdsArrowClient
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.tests.integrationV2.conftest import inside_ci


@pytest.fixture(scope="package")
def gds_plugin_container(
    logs_dir: Path, tmp_path_factory: pytest.TempPathFactory
) -> Generator[Neo4jContainer, None, None]:
    neo4j_image = os.getenv("NEO4J_DATABASE_IMAGE", "neo4j:enterprise")

    dotenv.load_dotenv("graphdatascience/tests/test.env", override=True)
    GDS_LICENSE_KEY = os.getenv("GDS_LICENSE_KEY")

    db_logs_dir = logs_dir / "cypher_surface" / "db_logs"
    db_logs_dir.mkdir(parents=True)
    db_logs_dir.chmod(0o777)

    neo4j_container = (
        Neo4jContainer(
            image=neo4j_image,
        )
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        .with_env("NEO4J_PLUGINS", '["graph-data-science"]')
        .with_env("NEO4J_gds_arrow_enabled", "true")
        .with_env("NEO4J_gds_arrow_listen__address", "0.0.0.0:8491")
        .with_exposed_ports(8491)
        .with_volume_mapping(db_logs_dir, "/logs", mode="rw")
    )

    if GDS_LICENSE_KEY is not None:
        license_dir = tmp_path_factory.mktemp("gds_license")
        license_dir.chmod(0o755)
        license_file = os.path.join(license_dir, "license_key")
        with open(license_file, "w") as f:
            f.write(GDS_LICENSE_KEY)

        neo4j_container.with_volume_mapping(
            license_dir,
            "/licenses",
        )
        neo4j_container.with_env("NEO4J_gds_enterprise_license__file", "/licenses/license_key")

    with neo4j_container as neo4j_db:
        wait_for_logs(neo4j_db, "Started.")
        yield neo4j_db
        stdout, stderr = neo4j_db.get_logs()
        if stderr:
            print(f"Error logs from Neo4j container:\n{stderr}")

        if inside_ci():
            print(f"Neo4j container logs:\n{stdout}")

        out_file = db_logs_dir / "stdout.log"
        with open(out_file, "w") as f:
            f.write(stdout.decode("utf-8"))


@pytest.fixture(scope="package")
def query_runner(gds_plugin_container: DockerContainer) -> Generator[QueryRunner, None, None]:
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
def gds_arrow_client(
    gds_plugin_container: DockerContainer, query_runner: QueryRunner
) -> Generator[GdsArrowClient, None, None]:
    arrow_port = int(gds_plugin_container.get_exposed_port(8491))
    with GdsArrowClient(
        gds_plugin_container.get_container_host_ip(),
        arrow_port,
        ("neo4j", "password"),
        encrypted=False,
    ) as client:
        yield client
