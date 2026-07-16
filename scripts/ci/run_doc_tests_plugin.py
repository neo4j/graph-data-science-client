import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from testcontainers.neo4j import Neo4jContainer

PASSWORD = "password"


def _resolve_license_dir() -> Path | None:
    """Return a host dir with a `license_key` file to mount at /licenses, or None if no license is available."""
    key = os.environ.get("GDS_LICENSE_KEY")
    if key:
        license_dir = Path(tempfile.mkdtemp(prefix="gds_license_"))
        license_dir.chmod(0o755)
        (license_dir / "license_key").write_text(key)
        return license_dir
    return None


def main() -> None:
    # A GDS license enables the enterprise scope; without one we fall back to community only.
    license_dir = _resolve_license_dir()
    enterprise = license_dir is not None

    image = os.environ.get("NEO4J_IMAGE", "neo4j:enterprise" if enterprise else "neo4j:latest")

    print(
        f"Running plugin doc tests against {image} "
        + (
            "(community + enterprise + networkx; license found)"
            if enterprise
            else "(community + networkx; no license found)"
        ),
        flush=True,
    )

    models_dir = Path(tempfile.mkdtemp("models"))
    models_dir.chmod(0o777)
    exports_dir = Path(tempfile.mkdtemp("exports"))
    exports_dir.chmod(0o777)

    container = (
        Neo4jContainer(image=image, password=PASSWORD)
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        .with_env("NEO4J_PLUGINS", '["graph-data-science"]')
        .with_env("NEO4J_gds_model_store__location", "/models")
        .with_env("NEO4J_gds_export_location", "/exports")
        .with_volume_mapping(models_dir, "/models", mode="rw")
        .with_volume_mapping(exports_dir, "/exports", mode="rw")
    )

    if license_dir is not None:
        container = container.with_volume_mapping(license_dir, "/licenses").with_env(
            "NEO4J_gds_enterprise_license__file", "/licenses/license_key"
        )

    try:
        with container as neo4j:
            uri = f"bolt://{neo4j.get_container_host_ip()}:{neo4j.get_exposed_port(7687)}"
            env = {
                **os.environ,
                "NEO4J_URI": uri,
                "NEO4J_USERNAME": "neo4j",
                "NEO4J_PASSWORD": PASSWORD,
            }
            # The doc-test harness runs each snippet with this Python interpreter (has graphdatascience + networkx).
            cmd = ["bundle", "exec", "ruby", "test_docs.rb", sys.executable]
            if not enterprise:
                cmd += ["-n", "/community|networkx/"]

            subprocess.run(["bundle", "install"], cwd="doc/tests", check=True, env=env)
            subprocess.run(cmd, cwd="doc/tests", check=True, env=env)
    finally:
        shutil.rmtree(models_dir)
        shutil.rmtree(exports_dir)
        if license_dir is not None:
            shutil.rmtree(license_dir)


if __name__ == "__main__":
    main()
