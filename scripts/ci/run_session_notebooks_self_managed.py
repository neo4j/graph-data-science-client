import logging
import os
import signal
import socket
from datetime import datetime
from pathlib import Path
from types import FrameType
from typing import Optional

from aura_api_ci import AuraApiCI
from dateutil.relativedelta import relativedelta
from testcontainers.core.container import DockerContainer
from testcontainers.core.docker_client import DockerClient
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Run the self-managed family of session notebooks against a local Neo4j started via
# testcontainers (instead of provisioning an AuraDB). The GDS session itself is still a real
# cloud (staging) session created through the Aura API, so AURA_API_CLIENT_ID/SECRET are
# required. Only `graph-analytics-serverless-self-managed.ipynb` uses the database; the
# `standalone` and `spark` notebooks ignore the NEO4J_* env vars.
#
# This script is invoked via plain `uv run` (default `dev` group), which includes the `test`
# group where `testcontainers` lives.

REPO_ROOT = Path(__file__).resolve().parents[2]


def inside_ci() -> bool:
    return os.environ.get("BUILD_ID") is not None


def _current_container_id() -> Optional[str]:
    """Detect: are we running inside a docker container that the sibling docker daemon knows
    about? Returns its id, or None for host runs. Mirrors tests/integration/conftest.py."""
    candidate = os.environ.get("TEST_CONTAINER_ID") or socket.gethostname()
    logger.info("resolving self container id via candidate=%r", candidate)
    if not candidate:
        return None
    try:
        container = DockerClient().client.containers.get(candidate)
    except Exception as e:
        logger.info("daemon could not find container %r: %s", candidate, e)
        return None
    logger.info("resolved current container id: %s", container.id)
    return str(container.id)


def latest_neo4j_version() -> str:
    today = datetime.now()

    previous_month = today - relativedelta(months=1)

    overrides = {"2025.12.0": "2025.12.1-1", "2026.01.0": "2026.01.2"}

    cal_ver = previous_month.strftime("%Y.%m.0")

    return overrides.get(cal_ver, cal_ver)


def main() -> None:
    aura_api = AuraApiCI.from_env()
    client_id = aura_api.client_id
    client_secret = aura_api.client_secret
    project_id = aura_api.get_tenant_id()

    logger.info("Using project_id=%s", project_id)

    default_neo4j_image = (
        f"europe-west1-docker.pkg.dev/neo4j-aura-image-artifacts/aura-dev/neo4j-enterprise:{latest_neo4j_version()}"
    )
    neo4j_image = os.getenv("NEO4J_DATABASE_IMAGE", default_neo4j_image)
    advertise_address = "neo4j-db" if inside_ci() else "localhost"

    self_id = _current_container_id()
    if self_id is None and inside_ci():
        raise RuntimeError(
            "Running inside CI (BUILD_ID is set) but could not determine this process's docker "
            "container id; the notebook process must be attachable to the testcontainers network. "
            "Set TEST_CONTAINER_ID in the build step or run the container with a `--name` that "
            "matches its hostname."
        )

    with Network() as network:
        if self_id is not None:
            logger.info("attaching %s to test network %s", self_id[:12], network.name)
            network.connect(self_id)
        try:
            db_container = (
                DockerContainer(image=neo4j_image)
                .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
                .with_env("NEO4J_AUTH", "neo4j/password")
                .with_env("NEO4J_server_jvm_additional", "-Dcom.neo4j.arrow.GdsFeatureToggles.enableGds=false")
                .with_env("NEO4J_server_bolt_advertised__address", f"{advertise_address}:7687")
                .with_network_aliases("neo4j-db")
                .with_network(network)
                .with_bind_ports(7687, 7687)
                .waiting_for(LogMessageWaitStrategy("Started."))
            )

            with db_container as db_container:
                if self_id is not None:
                    # The notebook process is attached to the test network: reach the DB by alias.
                    neo4j_uri = "neo4j://neo4j-db:7687"
                else:
                    host = db_container.get_container_host_ip()
                    port = db_container.get_exposed_port(7687)
                    neo4j_uri = f"neo4j://{host}:{port}"
                logger.info("Neo4j reachable at %s", neo4j_uri)

                # Teardown happens via the context managers and the finally below; signals just
                # trigger an early, clean exit.
                def handle_signal(sig: int, frame: FrameType | None) -> None:
                    logger.info("Received SIGNAL, tearing down Neo4j container")
                    raise KeyboardInterrupt

                signal.signal(signal.SIGINT, handle_signal)
                signal.signal(signal.SIGTERM, handle_signal)

                cmd = (
                    f"AURA_ENV=staging CLIENT_ID={client_id} CLIENT_SECRET={client_secret} PROJECT_ID={project_id} "
                    f"NEO4J_URI={neo4j_uri} NEO4J_USERNAME=neo4j NEO4J_PASSWORD=password "
                    f"uv run --group notebook-aura-ci ./scripts/run_notebooks.py sessions-self-managed-db"
                )

                if os.system(f"cd {REPO_ROOT} && {cmd}") != 0:
                    raise Exception("Failed to run self-managed session notebooks")

                logger.info("Self-managed session notebooks ran successfully")
        finally:
            if self_id is not None:
                try:
                    network._unwrap_network.disconnect(self_id)
                    logger.info("detached %s from test network %s", self_id[:12], network.name)
                except Exception as e:
                    logger.info("failed to detach %s from test network: %s", self_id[:12], e)


if __name__ == "__main__":
    main()
