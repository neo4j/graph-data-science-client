"""Run the AGA (Aura Graph Analytics) doc tests against a local GDS session.

Starts the same Docker stack as the integration tests (mock GDS API, Neo4j, and a
GDS session image, via tests/integration/services.py), then invokes the Ruby doc-test
harness (doc/tests/test_docs.rb) with its AGA lane: the Python snippets of the manual
that live in Aura Graph Analytics tabs, carry the `session` attribute, or are untabbed
in AGA files, all executed against the local session.

Image overrides work as in the integration tests: GDS_SESSION_IMAGE,
NEO4J_AURA_DATABASE_IMAGE, and MOCK_GDS_API_IMAGE. Use DOC_TEST_FILE=<substring> to
iterate on a single manual page.
"""

import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from testcontainers.core.network import Network

from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from tests.integration.services import (
    GdsSessionConnectionInfo,
    current_container_id,
    db_alias,
    inside_ci,
    session_alias,
    start_database,
    start_gds_api,
    start_session,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
LOG_NAME = "aga_doc_tests"
DOC_TESTS_DIR = REPO_ROOT / "doc" / "tests"


@contextmanager
def _gds_api(logs_dir: Path, network: Network) -> Generator[str, None, None]:
    yield from start_gds_api(logs_dir, network, LOG_NAME)


@contextmanager
def _database(logs_dir: Path, network: Network) -> Generator[DbmsConnectionInfo, None, None]:
    yield from start_database(logs_dir, network, LOG_NAME, db_alias=db_alias())


@contextmanager
def _session(
    logs_dir: Path, models_dir: Path, network: Network, gds_api_uri: str
) -> Generator[GdsSessionConnectionInfo, None, None]:
    yield from start_session(
        logs_dir,
        models_dir,
        network,
        LOG_NAME,
        gds_api_uri=gds_api_uri,
        session_alias=session_alias(),
    )


@contextmanager
def aga_session_stack(
    logs_dir: Path, models_dir: Path
) -> Generator[tuple[DbmsConnectionInfo, GdsSessionConnectionInfo], None, None]:
    """The local AGA test stack: docker network, mock GDS API, Neo4j database, GDS session.

    Composes the integration-test services (tests/integration/services.py) the same way
    the pytest fixtures do. Yields the Neo4j connection info and the session connection info.
    """
    with Network() as network:
        self_id = current_container_id()
        if self_id is not None:
            print(f"[aga-docs] attaching {self_id[:12]} to test network {network.name}", flush=True)
            network.connect(self_id)
        elif inside_ci():
            raise RuntimeError(
                "Running inside CI (BUILD_ID is set) but could not determine this process's "
                "docker container id; the doc-test processes must be attachable to the "
                "testcontainers network. Set TEST_CONTAINER_ID in the build step or run the "
                "container with a `--name` that matches its hostname."
            )
        try:
            with (
                _gds_api(logs_dir, network) as gds_api_uri,
                _database(logs_dir, network) as db_connection,
                _session(logs_dir, models_dir, network, gds_api_uri) as session,
            ):
                yield db_connection, session
        finally:
            if self_id is not None:
                try:
                    network._unwrap_network.disconnect(self_id)
                    print(f"[aga-docs] detached {self_id[:12]} from test network", flush=True)
                except Exception as e:
                    print(f"[aga-docs] failed to detach {self_id[:12]} from test network: {e}", flush=True)


def main() -> None:
    session_image = os.environ.get(
        "GDS_SESSION_IMAGE", "europe-west1-docker.pkg.dev/gds-aura-artefacts/gds/gds-session:aura-release"
    )
    print(f"Running AGA doc tests against {session_image}", flush=True)

    logs_dir = Path(tempfile.mkdtemp(prefix="aga_doc_logs_"))
    models_dir = Path(tempfile.mkdtemp(prefix="aga_doc_models_"))
    models_dir.chmod(0o777)  # the session container writes here as a different user

    try:
        with aga_session_stack(logs_dir, models_dir) as (db_connection, session):
            advertised_host, advertised_port = session.advertised_address
            env = {
                **os.environ,
                "NEO4J_URI": f"bolt://{db_connection.uri}",
                "NEO4J_USERNAME": "neo4j",
                "NEO4J_PASSWORD": "password",
                "GDS_SESSION_ARROW_URI": f"{session.host}:{session.arrow_port}",
                "GDS_SESSION_ADVERTISED_ADDRESS": f"{advertised_host}:{advertised_port}",
            }
            # The doc-test harness runs each snippet with this Python interpreter
            # (has graphdatascience + its dependencies installed).
            cmd = ["bundle", "exec", "ruby", "test_docs.rb", sys.executable, "-n", "test_aga"]
            subprocess.run(["bundle", "install"], cwd=DOC_TESTS_DIR, check=True, env=env)
            subprocess.run(cmd, cwd=DOC_TESTS_DIR, check=True, env=env)
    finally:
        shutil.rmtree(logs_dir, ignore_errors=True)
        shutil.rmtree(models_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
