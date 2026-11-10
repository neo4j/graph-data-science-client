import logging
import os
import subprocess
from pathlib import Path
from typing import Generator

import pytest

from graphdatascience.query_runner import QueryRunner
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from tests.integration.procedure_surface.api_spec import ApiSpec
from tests.integration.procedure_surface.api_spec import (
    resolve_spec_from_file as resolve_api_spec_from_file,
)
from tests.integration.procedure_surface.gds_api_spec import (
    EndpointWithModesSpec,
    resolve_spec_from_file,
)
from tests.integration.services import create_db_query_runner

LOGGER = logging.getLogger(__name__)

SESSION_API_SPEC_GCS_URI = "gs://gds-aura-artifacts-schemas/endpoints/gds-session-api-spec.json"
PLUGIN_API_SPEC_GCS_URI_PREFIX = "gs://gds-aura-artifacts-schemas/endpoints/gds-plugin-api-spec-v"


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


@pytest.fixture(scope="session")
def session_api_spec(tmp_path_factory: pytest.TempPathFactory) -> Generator[ApiSpec, None, None]:
    provided_spec_file = os.environ.get("GDS_SESSION_API_SPEC_FILE")

    spec_file: Path | None = None
    if provided_spec_file:
        spec_file = Path(provided_spec_file)

    if spec_file and not spec_file.exists():
        raise FileNotFoundError(f"GDS_SESSION_API_SPEC_FILE is set to '{spec_file}', but the file does not exist.")

    if not spec_file:
        spec_dir = tmp_path_factory.mktemp("session_api_spec")
        spec_file = spec_dir / "gds-session-api-spec.json"
        download_session_api_spec(spec_file)

    yield resolve_api_spec_from_file(spec_file)


@pytest.fixture(scope="session")
def plugin_api_spec(tmp_path_factory: pytest.TempPathFactory) -> Generator[ApiSpec, None, None]:
    provided_spec_file = os.environ.get("GDS_PLUGIN_API_SPEC_FILE")

    spec_file: Path | None = None
    if provided_spec_file:
        spec_file = Path(provided_spec_file)

    if spec_file and not spec_file.exists():
        raise FileNotFoundError(f"GDS_PLUGIN_API_SPEC_FILE is set to '{spec_file}', but the file does not exist.")

    if not spec_file:
        spec_dir = tmp_path_factory.mktemp("plugin_api_spec")
        spec_file = spec_dir / "gds-plugin-api-spec.json"
        download_plugin_api_spec(spec_file)

    yield resolve_api_spec_from_file(spec_file)


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


def download_session_api_spec(destination: Path) -> None:
    try:
        subprocess.run(
            ["gcloud", "storage", "cp", SESSION_API_SPEC_GCS_URI, str(destination)],
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError as e:
        raise ValueError(
            "Could not download the GDS Sessions API spec: `gcloud` is not installed. Either set "
            "`GDS_SESSION_API_SPEC_FILE` to a local copy or install `gcloud` with access to "
            f"`{SESSION_API_SPEC_GCS_URI}`."
        ) from e
    except subprocess.CalledProcessError as e:
        # Surface gcloud's stderr: it separates missing credentials from missing permissions.
        detail = (e.stderr or "").strip() or str(e)
        raise ValueError(
            f"Could not download the GDS Sessions API spec: `gcloud storage cp` failed ({detail}). Either set "
            "`GDS_SESSION_API_SPEC_FILE` to a local copy or authenticate `gcloud` with access to "
            f"`{SESSION_API_SPEC_GCS_URI}`."
        ) from e


def download_plugin_api_spec(destination: Path) -> None:
    # The plugin spec is published per GDS release under a versioned name; take the newest.
    try:
        listing = subprocess.run(
            ["gcloud", "storage", "ls", f"{PLUGIN_API_SPEC_GCS_URI_PREFIX}*.json"],
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError as e:
        raise ValueError(
            "Could not download the GDS plugin API spec: `gcloud` is not installed. Either set "
            "`GDS_PLUGIN_API_SPEC_FILE` to a local copy or install `gcloud` with access to "
            f"`{PLUGIN_API_SPEC_GCS_URI_PREFIX}*.json`."
        ) from e
    except subprocess.CalledProcessError as e:
        # Surface gcloud's stderr: it separates missing credentials from missing permissions.
        detail = (e.stderr or "").strip() or str(e)
        raise ValueError(
            f"Could not download the GDS plugin API spec: `gcloud storage ls` failed ({detail}). Either set "
            "`GDS_PLUGIN_API_SPEC_FILE` to a local copy or authenticate `gcloud` with access to "
            f"`{PLUGIN_API_SPEC_GCS_URI_PREFIX}*.json`."
        ) from e

    uris = [uri for uri in listing.stdout.split() if uri]
    if not uris:
        raise ValueError(
            "Could not download the GDS plugin API spec: no versioned spec found under "
            f"`{PLUGIN_API_SPEC_GCS_URI_PREFIX}*.json`."
        )

    def version_key(uri: str) -> tuple[int, ...]:
        # versioned names sort badly as plain strings (v2026.9.0 > v2026.10.0)
        version = uri.removeprefix(PLUGIN_API_SPEC_GCS_URI_PREFIX).removesuffix(".json")
        return tuple(int(part) for part in version.split("."))

    newest = max(uris, key=version_key)

    try:
        subprocess.run(
            ["gcloud", "storage", "cp", newest, str(destination)],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        # Surface gcloud's stderr: it separates missing credentials from missing permissions.
        detail = (e.stderr or "").strip() or str(e)
        raise ValueError(
            f"Could not download the GDS plugin API spec: `gcloud storage cp` failed ({detail}). Either set "
            "`GDS_PLUGIN_API_SPEC_FILE` to a local copy or authenticate `gcloud` with access to "
            f"`{newest}`."
        ) from e


@pytest.fixture(scope="package")
def query_runner(aura_neo4j_connection: DbmsConnectionInfo) -> Generator[QueryRunner, None, None]:
    yield from create_db_query_runner(aura_neo4j_connection)
