import os
import subprocess
from pathlib import Path
from typing import Generator

import pytest
from testcontainers.core.network import Network

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.tests.integrationV2.procedure_surface.conftest import (
    GdsSessionConnectionInfo,
    create_arrow_client,
    create_db_query_runner,
    start_database,
    start_session,
)
from graphdatascience.tests.integrationV2.procedure_surface.session.gds_api_spec import (
    EndpointWithModesSpec,
    resolve_spec_from_file,
)


@pytest.fixture(scope="package")
def session_connection(
    network: Network, password_dir: Path, logs_dir: Path
) -> Generator[GdsSessionConnectionInfo, None, None]:
    yield from start_session(logs_dir, network, password_dir)


@pytest.fixture(scope="package")
def arrow_client(session_connection: GdsSessionConnectionInfo) -> AuthenticatedArrowClient:
    return create_arrow_client(session_connection)


@pytest.fixture(scope="package")
def neo4j_connection(network: Network, logs_dir: Path) -> Generator[DbmsConnectionInfo, None, None]:
    yield from start_database(logs_dir, network)


@pytest.fixture(scope="package")
def db_query_runner(neo4j_connection: DbmsConnectionInfo) -> Generator[QueryRunner, None, None]:
    yield from create_db_query_runner(neo4j_connection)


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
