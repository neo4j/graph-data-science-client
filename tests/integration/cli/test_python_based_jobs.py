"""End-to-end matrix test for CLI jobs whose algorithms need the python-runtime.

Some algorithms (e.g. FastPath) only run on a session wired to the python-runtime
API, so they use the ``gds_runtime`` fixture (see conftest) rather than the plain
``gds`` used by ``test_java_based_jobs.py``. Everything else is identical: drive the
real ``steps.run_all`` path on a real job + construct graph and assert results +
writebacks.

Adding coverage is a pure data edit: drop a ``testdata/python-jobs/<name>.yaml`` (a
projection's ``graph_name`` names the ``testdata/graphs/<graph_name>.json`` to upload).
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
JOBS = sorted((TESTDATA / "python-jobs").glob("*.yaml"))


@pytest.mark.db_integration
# FastPath emits a "preview feature" UserWarning on construction; pytest.ini turns
# warnings into errors, so ignore it for these jobs.
@pytest.mark.filterwarnings("ignore:FastPath is a preview feature:UserWarning")
@pytest.mark.parametrize("job_path", JOBS, ids=[p.stem for p in JOBS])
def test_python_based_job(job_path: Path, gds_runtime: AuraGraphDataScience, db_config: DatabaseConfig) -> None:
    cfg = JobConfig.from_file(str(job_path))

    client = DatabaseClient.from_config(db_config)
    for graph_name in {p.graph_name for p in cfg.projections}:
        client.upload(graph_from_file(GRAPHS_DIR / f"{graph_name}.json"), overwrite=True, show_progress=False)

    result = run_all(gds_runtime, cfg, overwrite_graph=True, report=JobReport(quiet=True))

    assert len(result["algorithms"]) == len(cfg.algorithms)
    assert all(item["result"] is not None for item in result["algorithms"])

    for wb in cfg.writebacks:
        for prop in wb.node_properties:
            count = cast(
                int,
                gds_runtime.run_cypher(f"MATCH (n:Dev) WHERE n.`{prop}` IS NOT NULL RETURN count(n) AS c").squeeze(),
            )
            assert count > 0, f"{job_path.stem}: writeback '{prop}' did not persist to the database"
