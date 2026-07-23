"""End-to-end matrix test for the ``gds`` CLI job runner against live containers.

Covers the Java-based GDS algorithms that run on the plain session (the ``gds``
fixture). Python-runtime algorithms (e.g. FastPath) live in
``test_python_based_jobs.py`` because they need a different session.

Each case drives the *real* execution path (``steps.run_all`` - what ``gds session run``
calls) with a *real* job config and construct-format graph, only skipping the Aura
session-creation code (see ``conftest.gds``). For every job we:

1. upload the graph(s) it needs into the Neo4j container (construct format), then
2. project remotely + run the algorithm(s) + write results back via ``run_all``, then
3. assert every compute ran and every written property landed in the DB.

Adding coverage is a pure data edit: drop a ``testdata/jobs/<name>.yaml`` carrying a
``# graphs: <name>[, <name>]`` directive naming the ``testdata/graphs/<name>.json``
graph(s) to upload (the new schema has no per-job graph name).

Algorithms that require an UNDIRECTED projection (triangleCount, localClusteringCoefficient)
declare ``undirectedRelationshipTypes`` on their projection - a field on ``ProjectSpec``
threaded through ``steps._project_one`` into ``CatalogArrowEndpoints.project``.
"""

import re
from pathlib import Path
from typing import cast

import pytest
from gds_cli.common.env import DatabaseConfig
from gds_cli.database.construct import graph_from_file
from gds_cli.database.db import DatabaseClient
from gds_cli.session.config import JobsConfig
from gds_cli.session.report import JobReport
from gds_cli.session.steps import run_all

from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience

TESTDATA = Path(__file__).parent / "testdata"
GRAPHS_DIR = TESTDATA / "graphs"
JOBS = sorted((TESTDATA / "jobs").glob("*.yaml"))


def required_graphs(job_path: Path) -> list[str]:
    """Graph JSON names to upload, from the job file's ``# graphs: a, b`` directive."""
    for line in job_path.read_text().splitlines():
        match = re.match(r"#\s*graphs:\s*(.+)", line)
        if match:
            return [name.strip() for name in match.group(1).split(",") if name.strip()]
    raise ValueError(f"{job_path.name} needs a `# graphs: <name>[, <name>]` directive.")


@pytest.mark.db_integration
@pytest.mark.parametrize("job_path", JOBS, ids=[p.stem for p in JOBS])
def test_java_based_job(job_path: Path, gds: AuraGraphDataScience, db_config: DatabaseConfig) -> None:
    cfg = JobsConfig.from_file(str(job_path))

    # Upload every graph this job needs. The DB + session catalog were already
    # wiped upfront by the autouse `clean_state` fixture, so this is a clean load.
    client = DatabaseClient.from_config(db_config)
    for graph_name in required_graphs(job_path):
        graph = graph_from_file(GRAPHS_DIR / f"{graph_name}.json")
        client.upload(graph, overwrite=True, show_progress=False)

    result = run_all(gds, cfg, overwrite_graph=True, report=JobReport(quiet=True))

    # (1) every configured compute ran.
    assert len(result["computes"]) == sum(len(job.compute) for job in cfg.jobs)

    # (2) every written property landed on the (uploaded) nodes in the DB. Uploaded
    # nodes all carry the `Dev` extra label, so we can count against it regardless of
    # the business label.
    for job in cfg.jobs:
        for write in job.write:
            count = cast(
                int,
                gds.run_cypher(f"MATCH (n:Dev) WHERE n.`{write.target}` IS NOT NULL RETURN count(n) AS c").squeeze(),
            )
            assert count > 0, f"{job_path.stem}: write '{write.target}' did not persist to the database"
