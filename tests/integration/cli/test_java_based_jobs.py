"""End-to-end matrix test for the ``gds`` CLI job runner against live containers.

Covers the Java-based GDS algorithms that run on the plain session (the ``gds``
fixture). Python-runtime algorithms (e.g. FastPath) live in
``test_python_based_jobs.py`` because they need a different session.

Each case drives the *real* execution path (``steps.run_all`` - what ``gds session run``
calls) with a *real* job config and construct-format graph, only skipping the Aura
session-creation code (see ``conftest.gds``). For every job we:

1. upload the graph(s) it projects into the Neo4j container (construct format), then
2. project remotely + run the algorithm(s) + write results back via ``run_all``, then
3. assert every algorithm returned a result and every writeback landed in the DB.

Adding coverage is a pure data edit: drop a ``testdata/jobs/<name>.yaml`` (a projection's
``graph_name`` names the ``testdata/graphs/<graph_name>.json`` to upload).

Algorithms that require an UNDIRECTED projection (triangleCount, localClusteringCoefficient)
declare ``undirected_relationship_types`` on their projection - a field on ``ProjectionConfig``
threaded through ``steps._project_one`` into ``CatalogArrowEndpoints.project``.
"""

from pathlib import Path
from typing import cast

import pytest
from gds_cli.common.env import DatabaseConfig
from gds_cli.database.construct import graph_from_file
from gds_cli.database.db import DatabaseClient
from gds_cli.session.config import JobConfig
from gds_cli.session.report import JobReport
from gds_cli.session.steps import run_all

from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience

TESTDATA = Path(__file__).parent / "testdata"
GRAPHS_DIR = TESTDATA / "graphs"
JOBS = sorted((TESTDATA / "jobs").glob("*.yaml"))


@pytest.mark.db_integration
@pytest.mark.parametrize("job_path", JOBS, ids=[p.stem for p in JOBS])
def test_java_based_job(job_path: Path, gds: AuraGraphDataScience, db_config: DatabaseConfig) -> None:
    cfg = JobConfig.from_file(str(job_path))

    # Upload every graph this job projects. The DB + session catalog were already
    # wiped upfront by the autouse `clean_state` fixture, so this is a clean load.
    client = DatabaseClient.from_config(db_config)
    for graph_name in {p.graph_name for p in cfg.projections}:
        graph = graph_from_file(GRAPHS_DIR / f"{graph_name}.json")
        client.upload(graph, overwrite=True, show_progress=False)

    result = run_all(gds, cfg, overwrite_graph=True, report=JobReport(quiet=True))

    # (1) every configured algorithm ran and produced a result object.
    assert len(result["algorithms"]) == len(cfg.algorithms)
    assert all(item["result"] is not None for item in result["algorithms"])

    # (2) every writeback property landed on the (uploaded) nodes in the DB. Uploaded
    # nodes all carry the `Dev` extra label, so we can count against it regardless of
    # the business label.
    for wb in cfg.writebacks:
        for prop in wb.node_properties:
            count = cast(
                int, gds.run_cypher(f"MATCH (n:Dev) WHERE n.`{prop}` IS NOT NULL RETURN count(n) AS c").squeeze()
            )
            assert count > 0, f"{job_path.stem}: writeback '{prop}' did not persist to the database"
