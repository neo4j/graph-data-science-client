import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Generator

import dotenv
import pytest
from dateutil.relativedelta import relativedelta
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.neo4j import Neo4jContainer

from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from tests.integrationV2.conftest import inside_ci
from tests.integrationV2.procedure_surface.gds_api_spec import (
    EndpointWithModesSpec,
    resolve_spec_from_file,
)

LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def gds_api_spec(tmp_path_factory: pytest.TempPathFactory) -> Generator[list[EndpointWithModesSpec], None, None]:
    provided_spec_file = os.environ.get("GDS_API_SPEC_FILE")

    spec_file: Path | None = None
    if provided_spec_file:
        spec_file = Path(provided_spec_file)

    if spec_file and not spec_file.exists():
        raise FileNotFoundError(f"GDS_API_SPEC_FILE is set to '{spec_file}', but the file does not exist.")

    if not spec_file:
        spec_dir = tmp_path_factory.mktemp("gds_api_spec")
        spec_file = spec_dir / "gds-api-spec.json"

        # allow for caching
        if not spec_file.exists():
            download_gds_api_spec(spec_file)

    # Adjust the path to pull from graph-analytics
    yield resolve_spec_from_file(spec_file)


def download_gds_api_spec(destination: Path) -> None:
    import requests

    url = "https://raw.githubusercontent.com/neo-technology/graph-analytics/refs/heads/master/tools/gds-api-spec/gds-api-spec.json"
    gh_token = os.environ.get("GITHUB_TOKEN")
    if not gh_token:
        try:
            result = subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, check=True)
            gh_token = result.stdout.strip()
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            raise ValueError("Failed to get GitHub token. Set GITHUB_TOKEN or authenticate with gh CLI.") from e

    headers = {"Authorization": f"Token {gh_token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    with open(destination, "wb") as f:
        f.write(response.content)


def latest_neo4j_version() -> str:
    today = datetime.now()
    previous_month = today - relativedelta(months=1)
    return previous_month.strftime("%Y.%m.0")


def start_database(logs_dir: Path, network: Network) -> Generator[DbmsConnectionInfo, None, None]:
    default_neo4j_image = (
        f"europe-west1-docker.pkg.dev/neo4j-aura-image-artifacts/aura/neo4j-enterprise:{latest_neo4j_version()}"
    )
    neo4j_image = os.getenv("NEO4J_DATABASE_IMAGE", default_neo4j_image)
    if neo4j_image is None:
        raise ValueError("NEO4J_DATABASE_IMAGE environment variable is not set")
    db_logs_dir = logs_dir / "arrow_surface" / "db_logs"
    db_logs_dir.mkdir(parents=True, exist_ok=True)
    db_logs_dir.chmod(0o777)
    db_container = (
        DockerContainer(image=neo4j_image)
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        .with_env("NEO4J_AUTH", "neo4j/password")
        .with_env("NEO4J_server_jvm_additional", "-Dcom.neo4j.arrow.GdsFeatureToggles.enableGds=false")
        .with_env("NEO4J_server_bolt_advertised__address", "localhost:7687")
        .with_network_aliases("neo4j-db")
        .with_network(network)
        .with_bind_ports(7687, 7687)
        .with_volume_mapping(db_logs_dir, "/logs", mode="rw")
        .waiting_for(LogMessageWaitStrategy("Started."))
    )
    with db_container as db_container:
        try:
            yield DbmsConnectionInfo(
                uri=f"{db_container.get_container_host_ip()}:{db_container.get_exposed_port(7687)}",
                username="neo4j",
                password="password",
            )
        finally:
            stdout, stderr = db_container.get_logs()

            if stderr:
                print(f"Error logs from database container:\n{stderr}")

            if inside_ci():
                print(f"Database container logs:\n{stdout}")

            out_file = db_logs_dir / "stdout.log"
            with open(out_file, "w") as f:
                f.write(stdout.decode("utf-8"))


def start_gds_plugin_database(
    logs_dir: Path, tmp_path_factory: pytest.TempPathFactory
) -> Generator[Neo4jContainer, None, None]:
    neo4j_image = os.getenv("NEO4J_DATABASE_IMAGE", "neo4j:enterprise")

    dotenv.load_dotenv("tests/test.env", override=True)
    GDS_LICENSE_KEY = os.getenv("GDS_LICENSE_KEY")

    db_logs_dir = logs_dir / "cypher_surface" / "db_logs"
    db_logs_dir.mkdir(parents=True)
    db_logs_dir.chmod(0o777)

    models_dir = tmp_path_factory.mktemp("models")
    models_dir.chmod(0o777)

    neo4j_container = (
        Neo4jContainer(
            image=neo4j_image,
        )
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        .with_env("NEO4J_PLUGINS", '["graph-data-science"]')
        .with_env("NEO4J_gds_arrow_enabled", "true")
        .with_env("NEO4J_gds_arrow_listen__address", "0.0.0.0:8491")
        .with_env("NEO4J_gds_model_store__location", "/models")
        .with_exposed_ports(8491)
        .with_volume_mapping(db_logs_dir, "/logs", mode="rw")
        .with_volume_mapping(models_dir, "/models", mode="rw")
        .waiting_for(LogMessageWaitStrategy("Started."))
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
        try:
            yield neo4j_db
        finally:
            stdout, stderr = neo4j_db.get_logs()
            if stderr:
                print(f"Error logs from Neo4j container:\n{stderr}")

            if inside_ci():
                print(f"Neo4j container logs:\n{stdout}")

            out_file = db_logs_dir / "stdout.log"
            with open(out_file, "w") as f:
                f.write(stdout.decode("utf-8"))


def create_db_query_runner(neo4j_connection: DbmsConnectionInfo) -> Generator[Neo4jQueryRunner, None, None]:
    query_runner = Neo4jQueryRunner.create_for_db(
        f"bolt://{neo4j_connection.uri}",
        ("neo4j", "password"),
    )
    yield query_runner
    query_runner.close()
