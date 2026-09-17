"""Run the plugin-lane doc tests against a local Neo4j with the GDS plugin.

Starts a Neo4j+GDS-plugin container via tests/integration/services.py (the same setup as
the integration tests), then invokes the Ruby doc-test harness (doc/tests/test_docs.rb)
with its plugin deployments. Without a GDS license only the community-safe snippets run
(test_plugin_community); with GDS_LICENSE_KEY in the environment the enterprise snippets
are included (test_plugin_enterprise) and the GDS Arrow Flight server is enabled (it
requires the license), its host port pinned to 8491 and its address passed to the
harness via NEO4J_ARROW_URI. Image override via NEO4J_IMAGE. Use
DOC_TEST_FILE=<substring> to iterate on a single page.
"""

import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from testcontainers.community.neo4j import Neo4jContainer

from tests.integration.services import start_plugin_database

REPO_ROOT = Path(__file__).resolve().parents[2]
LOG_NAME = "plugin_doc_tests"
DOC_TESTS_DIR = REPO_ROOT / "doc" / "tests"
PASSWORD = "password"


@contextmanager
def _plugin_database(
    logs_dir: Path,
    models_dir: Path,
    image: str,
    gds_license_key: str | None,
    arrow_enabled: bool,
    arrow_host_port: int | None,
) -> Generator[Neo4jContainer, None, None]:
    yield from start_plugin_database(
        logs_dir, LOG_NAME, models_dir, image, gds_license_key, arrow_enabled, arrow_host_port
    )


def main() -> None:
    # A GDS license enables the enterprise scope; without one we fall back to community only.
    gds_license_key = os.environ.get("GDS_LICENSE_KEY")
    enterprise = gds_license_key is not None
    # The GDS Arrow Flight server requires the license: without one it never starts, so the
    # doc snippets (plain `GraphDataScience(NEO4J_URI, auth=...)`) must fall back to Bolt.
    arrow_enabled = enterprise
    # Pin the Arrow host port (instead of an ephemeral one) so that the address the server
    # advertises (`0.0.0.0:8491`) resolves to localhost:8491 on the Docker host. Doc
    # snippets such as the bookmarks example construct their own `GraphDataScience`
    # without an explicit arrow address and thus rely on that auto-discovered address.
    # Caveat: a pinned port conflicts with anything else binding 8491 on the host (e.g. a
    # concurrent arrow-enabled doc-test run, or the gds_session compose test env).
    arrow_host_port = 8491 if arrow_enabled else None

    image = os.environ.get("NEO4J_IMAGE", "neo4j:enterprise" if enterprise else "neo4j:latest")

    print(
        f"Running plugin doc tests against {image} "
        + (
            "(community + enterprise + networkx; license found, arrow enabled)"
            if enterprise
            else "(community + networkx; no license found, arrow disabled)"
        ),
        flush=True,
    )

    logs_dir = Path(tempfile.mkdtemp(prefix="plugin_doc_logs_"))
    models_dir = Path(tempfile.mkdtemp(prefix="plugin_doc_models_"))
    models_dir.chmod(0o777)  # the container writes stored models here as a different user

    try:
        with _plugin_database(logs_dir, models_dir, image, gds_license_key, arrow_enabled, arrow_host_port) as neo4j:
            env = {
                **os.environ,
                "NEO4J_URI": f"bolt://{neo4j.get_container_host_ip()}:{neo4j.get_exposed_port(7687)}",
                "NEO4J_USERNAME": "neo4j",
                "NEO4J_PASSWORD": PASSWORD,
            }
            if arrow_enabled:
                # The harness INIT connects the shared `gds` client via this explicit
                # address; snippet-level clients fall back to the advertised (pinned)
                # localhost:8491. Both routes reach the same endpoint.
                env["NEO4J_ARROW_URI"] = f"{neo4j.get_container_host_ip()}:{neo4j.get_exposed_port(8491)}"

            # The doc-test harness runs each snippet with this Python interpreter
            # (has graphdatascience + networkx). Only the plugin deployments are
            # selected (`/plugin/` matches test_plugin_community and
            # test_plugin_enterprise); the AGA deployment has its own runner and
            # Docker stack. Without a license only the community-safe snippets run.
            deployment_test = "/plugin/" if enterprise else "test_plugin_community"
            cmd = ["bundle", "exec", "ruby", "test_docs.rb", sys.executable, "-n", deployment_test]

            subprocess.run(["bundle", "install"], cwd=DOC_TESTS_DIR, check=True, env=env)
            subprocess.run(cmd, cwd=DOC_TESTS_DIR, check=True, env=env)
    finally:
        shutil.rmtree(logs_dir, ignore_errors=True)
        shutil.rmtree(models_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
