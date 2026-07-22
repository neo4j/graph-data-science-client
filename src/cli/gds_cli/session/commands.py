"""``gds session`` — create/list/delete a managed Aura GDS session, and run a
standardized GDS job (projections -> algorithms -> writebacks) against it.

Feed a standardized job config and run the whole pipeline (`run`) - projection
and drop of each graph happen implicitly, right before its first algorithm
reference and right after its last. Or run one step at a time (`project` /
`algorithms` / `writeback`) so each can be a separate k8s step; since that
splits execution across process boundaries, those steps are coarse-grained
instead: `project` projects every graph upfront, `writeback` writes back every
configured graph, and `drop` removes every projected graph so the session can
be reused for another experiment. Credentials come from the unified env set (see
:mod:`gds_cli.common.env`). A ``.env`` in the working directory is
loaded automatically; pass --env-file for another dotenv file. Real environment
variables always take precedence over dotenv files.

The job config itself is either a file (`--file`/`-f`) or, if omitted, a YAML
document in the $GDS_JOB_CONFIG env var — lets a single k8s Job resource carry
its config inline instead of needing a paired ConfigMap + volume mount.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import typer

if TYPE_CHECKING:
    from gds_cli.session.config import JobConfig
    from gds_cli.session.report import JobReport
    from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience

app = typer.Typer(
    help="Manage a GDS session and run a standardized GDS job (projections -> algorithms -> writebacks) against it.",
    no_args_is_help=True,
)

FILE_OPT = typer.Option(
    None, "--file", "-f", help="Path to the job config (YAML). If omitted, reads YAML from $GDS_JOB_CONFIG."
)
ENV_FILE_OPT = typer.Option(
    None,
    "--env-file",
    help="Extra dotenv file to read (a .env in the working dir loads automatically; real env vars win).",
)
PROGRESS_BAR_OPT = typer.Option(
    True,
    "--progress-bar/--no-progress-bar",
    help="Show the underlying job progress bar (projection, algorithms, writeback). On by default.",
)


def _load(file: Optional[str], env_file: Optional[str]) -> JobConfig:
    from gds_cli.common.env import load_env
    from gds_cli.session.config import JobConfig

    load_env(env_file)
    if file is not None:
        return JobConfig.from_file(file)
    return JobConfig.from_env()


def _connect(cfg: JobConfig, report: JobReport, progress_bar: bool = False) -> AuraGraphDataScience:
    from gds_cli.session.session_ops import connect, delete, find_session

    report.section("Connecting")
    with report.step(f"Connecting to session '{cfg.session.name}' ({cfg.session.memory})"):
        existing = find_session(cfg.session)
        if existing is not None and "delet" in existing.status.lower():
            report.note(f"Session '{cfg.session.name}' existed but is {existing.status.lower()} - it will be recreated")
            try:
                delete(cfg.session)
            except Exception:
                pass
            existing = None
        verb = "Reconnecting to existing" if existing else "Creating new"
        report.note(f"{verb} session '{cfg.session.name}' ({cfg.session.memory})")
        gds = connect(cfg.session, show_progress=progress_bar)
        session = existing or find_session(cfg.session)
        if session is not None:
            report.note(f"Session id: {session.id}")
    return gds


@app.command()
def create(
    file: Optional[str] = FILE_OPT, progress_bar: bool = PROGRESS_BAR_OPT, env_file: Optional[str] = ENV_FILE_OPT
) -> None:
    """Create (or reconnect to) the session defined in the config."""
    from gds_cli.session.report import JobReport

    cfg = _load(file, env_file)
    _connect(cfg, JobReport(), progress_bar=progress_bar)


@app.command()
def delete(file: Optional[str] = FILE_OPT, env_file: Optional[str] = ENV_FILE_OPT) -> None:
    """Delete the session defined in the config."""
    from gds_cli.session.session_ops import delete as delete_session

    cfg = _load(file, env_file)
    deleted = delete_session(cfg.session)
    if deleted:
        typer.secho(f"Deleted session '{cfg.session.name}'.", fg=typer.colors.GREEN)
    else:
        typer.echo(f"No session named '{cfg.session.name}' found.")


@app.command(name="list")
def list_(env_file: Optional[str] = ENV_FILE_OPT) -> None:
    """List every GDS session visible to the configured Aura API credentials."""
    from gds_cli.common.env import load_env
    from gds_cli.session.session_ops import list_sessions

    load_env(env_file)
    sessions = list_sessions()
    if not sessions:
        typer.echo("No sessions found.")
        return
    for info in sessions:
        typer.echo(f"{info.name}  id={info.id}  status={info.status}  memory={info.memory}  ttl={info.ttl}")


@app.command()
def run(
    file: Optional[str] = FILE_OPT,
    overwrite_graph: bool = typer.Option(
        False, "--overwrite-graph", help="Drop an existing same-named graph before projecting."
    ),
    delete_session: bool = typer.Option(
        False,
        "--delete-session",
        envvar="GDS_RUNNER_DELETE_SESSION",
        help="Delete the session once the job completes (e.g. for a one-off k8s Job).",
    ),
    progress_bar: bool = PROGRESS_BAR_OPT,
    env_file: Optional[str] = ENV_FILE_OPT,
) -> None:
    """Run the whole job: algorithms in order, each graph implicitly projected/written-back/dropped."""
    from gds_cli.session.report import JobReport
    from gds_cli.session.session_ops import delete as delete_session_op
    from gds_cli.session.steps import run_all

    cfg = _load(file, env_file)
    report = JobReport()
    gds = _connect(cfg, report, progress_bar=progress_bar)
    run_all(gds, cfg, overwrite_graph=overwrite_graph, report=report)
    if delete_session:
        delete_session_op(cfg.session)
        typer.echo(f"Deleted session '{cfg.session.name}'.")
    typer.secho("Job complete.", fg=typer.colors.GREEN)


@app.command()
def project(
    file: Optional[str] = FILE_OPT,
    overwrite_graph: bool = typer.Option(
        False, "--overwrite-graph", help="Drop an existing same-named graph before projecting."
    ),
    progress_bar: bool = PROGRESS_BAR_OPT,
    env_file: Optional[str] = ENV_FILE_OPT,
) -> None:
    """Step 1: project every graph declared in the config into the session."""
    from gds_cli.session.report import JobReport
    from gds_cli.session.steps import project_all

    cfg = _load(file, env_file)
    report = JobReport()
    gds = _connect(cfg, report, progress_bar=progress_bar)
    project_all(gds, cfg, overwrite=overwrite_graph, report=report)
    typer.secho("Projection complete.", fg=typer.colors.GREEN)


@app.command()
def algorithms(
    file: Optional[str] = FILE_OPT,
    only: Optional[str] = typer.Option(None, "--only", help="Run only the named algorithm from the list."),
    progress_bar: bool = PROGRESS_BAR_OPT,
    env_file: Optional[str] = ENV_FILE_OPT,
) -> None:
    """Step 2: run the ordered list of algorithms on their already-projected graphs."""
    from gds_cli.session.report import JobReport
    from gds_cli.session.steps import run_algorithms

    cfg = _load(file, env_file)
    report = JobReport()
    gds = _connect(cfg, report, progress_bar=progress_bar)
    run_algorithms(gds, cfg, only=only, report=report)
    typer.secho("Algorithms complete.", fg=typer.colors.GREEN)


@app.command()
def writeback(
    file: Optional[str] = FILE_OPT, progress_bar: bool = PROGRESS_BAR_OPT, env_file: Optional[str] = ENV_FILE_OPT
) -> None:
    """Step 3: write every configured graph's mutated node properties back to the database."""
    from gds_cli.session.report import JobReport
    from gds_cli.session.steps import run_writebacks

    cfg = _load(file, env_file)
    if not cfg.writebacks:
        typer.echo("No writebacks configured; nothing to do.")
        return
    report = JobReport()
    gds = _connect(cfg, report, progress_bar=progress_bar)
    run_writebacks(gds, cfg, report=report)
    typer.secho("Writeback complete.", fg=typer.colors.GREEN)


@app.command()
def drop(
    file: Optional[str] = FILE_OPT, progress_bar: bool = PROGRESS_BAR_OPT, env_file: Optional[str] = ENV_FILE_OPT
) -> None:
    """Drop every projected graph from the session, keeping the session for reuse."""
    from gds_cli.session.report import JobReport
    from gds_cli.session.steps import drop_all

    cfg = _load(file, env_file)
    report = JobReport()
    gds = _connect(cfg, report, progress_bar=progress_bar)
    drop_all(gds, cfg, report=report)
    typer.secho("Drop complete.", fg=typer.colors.GREEN)
