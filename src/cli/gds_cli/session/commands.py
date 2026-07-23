"""``gds sessions`` — create, list, and delete managed Aura GDS sessions, plus the
shared helpers behind ``gds run``.

``create`` and ``delete`` read the session definition from a job config
(``--file``/``-f``) and/or from CLI flags (``--name``/``--memory``/``--ttl``/
``--cloud``/``--region``); CLI flags override the config. ``list`` needs no config.
Aura credentials come from the unified env set (see :mod:`gds_cli.common.env`): a
``.env`` in the working directory is loaded automatically, ``--env-file`` reads
another dotenv file, and real environment variables always take precedence.

Running a job against a session is a separate top-level command, ``gds run`` (see
:mod:`gds_cli.run`), which manages the session lifecycle itself.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import typer

if TYPE_CHECKING:
    from gds_cli.session.config import JobsConfig, SessionConfig
    from gds_cli.session.report import JobReport
    from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience

app = typer.Typer(
    help="Create, list, and delete managed Aura GDS sessions.",
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
    help="Show the underlying client progress bars (projection, compute, write-back). On by default.",
)
NAME_OPT = typer.Option(None, "--name", "-n", help="Session name. Overrides `session.name` from --file.")
MEMORY_OPT = typer.Option(None, "--memory", help="Session memory, e.g. 2GB. Overrides the config.")
TTL_OPT = typer.Option(
    None, "--ttl", help="Session time-to-live: minutes (30) or a duration (30m/2h/1d). Overrides the config."
)
CLOUD_OPT = typer.Option(None, "--cloud", help="Cloud provider (gcp/aws/azure) for a STANDALONE session (+ --region).")
REGION_OPT = typer.Option(None, "--region", help="Cloud region for a standalone session (+ --cloud).")


def _load(file: Optional[str], env_file: Optional[str]) -> JobsConfig:
    from gds_cli.common.env import load_env
    from gds_cli.session.config import JobsConfig

    load_env(env_file)
    if file is not None:
        return JobsConfig.from_file(file)
    return JobsConfig.from_env()


def _connect(
    session: SessionConfig, session_name: str, report: JobReport, progress_bar: bool = False
) -> AuraGraphDataScience:
    """Create (or reconnect to) ``session_name`` for ``session``; reports as it goes.

    A session found in a "deleting" state is deleted and recreated, so a stale name
    never blocks the connect.
    """
    from gds_cli.session.session_ops import connect, delete, find_session

    report.section("Connecting")
    with report.step(f"Connecting to session '{session_name}' ({session.memory})"):
        existing = find_session(session_name)
        if existing is not None and "delet" in existing.status.lower():
            report.note(f"Session '{session_name}' existed but is {existing.status.lower()} - it will be recreated")
            try:
                delete(session_name)
            except Exception:
                pass
            existing = None
        verb = "Reconnecting to existing" if existing else "Creating new"
        report.note(f"{verb} session '{session_name}' ({session.memory})")
        gds = connect(session, session_name, show_progress=progress_bar)
        session_info = existing or find_session(session_name)
        if session_info is not None:
            report.note(f"Session id: {session_info.id}")
    return gds


def _resolve_session(
    file: Optional[str],
    env_file: Optional[str],
    *,
    name: Optional[str],
    memory: Optional[str],
    ttl: Optional[str],
    cloud: Optional[str],
    region: Optional[str],
) -> tuple[str, SessionConfig]:
    """Build ``(name, SessionConfig)`` from a config file and/or CLI flags.

    A ``--file`` supplies the defaults (its ``session:`` block); each CLI flag
    overrides the matching field. Requires a resolvable name plus memory and ttl.
    """
    from gds_cli.common.env import load_env
    from gds_cli.session.config import JobsConfig, SessionConfig

    load_env(env_file)
    fields: dict[str, object] = {}
    config_name: Optional[str] = None
    if file is not None:
        block = JobsConfig.from_file(file).session
        config_name = block.name
        fields = {
            "memory": block.memory,
            "ttl": block.ttl,
            "cloud": block.cloud,
            "region": block.region,
        }
    if memory is not None:
        fields["memory"] = memory
    if ttl is not None:
        fields["ttl"] = ttl
    if cloud is not None:
        fields["cloud"] = cloud
    if region is not None:
        fields["region"] = region

    resolved_name = name or config_name
    if not resolved_name:
        raise typer.BadParameter(
            "session name required: pass --name, or --file pointing at a config with session.name."
        )
    if "memory" not in fields or "ttl" not in fields:
        raise typer.BadParameter("session memory and ttl required: pass --memory and --ttl, or --file with a session.")
    return resolved_name, SessionConfig(**fields)  # validates cloud/region are both-or-neither


def _resolve_session_name(file: Optional[str], env_file: Optional[str], name: Optional[str]) -> str:
    """Resolve just a session name from ``--name`` or a config's ``session.name``."""
    from gds_cli.common.env import load_env
    from gds_cli.session.config import JobsConfig

    load_env(env_file)
    resolved = name
    if resolved is None and file is not None:
        resolved = JobsConfig.from_file(file).session.name
    if not resolved:
        raise typer.BadParameter(
            "session name required: pass --name, or --file pointing at a config with session.name."
        )
    return resolved


@app.command()
def create(
    file: Optional[str] = FILE_OPT,
    name: Optional[str] = NAME_OPT,
    memory: Optional[str] = MEMORY_OPT,
    ttl: Optional[str] = TTL_OPT,
    cloud: Optional[str] = CLOUD_OPT,
    region: Optional[str] = REGION_OPT,
    progress_bar: bool = PROGRESS_BAR_OPT,
    env_file: Optional[str] = ENV_FILE_OPT,
) -> None:
    """Create (or reconnect to) a session, from a config file and/or CLI flags."""
    from gds_cli.session.report import JobReport

    resolved_name, session = _resolve_session(
        file, env_file, name=name, memory=memory, ttl=ttl, cloud=cloud, region=region
    )
    _connect(session, resolved_name, JobReport(), progress_bar=progress_bar)
    typer.secho(f"Session '{resolved_name}' ready.", fg=typer.colors.GREEN)


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
def delete(
    file: Optional[str] = FILE_OPT,
    name: Optional[str] = NAME_OPT,
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt."),
    env_file: Optional[str] = ENV_FILE_OPT,
) -> None:
    """Delete a session by name (from --name or a config's session.name)."""
    from gds_cli.session.session_ops import delete as delete_session

    resolved_name = _resolve_session_name(file, env_file, name)
    if not yes:
        typer.confirm(f"Delete session '{resolved_name}'?", abort=True)
    if delete_session(resolved_name):
        typer.secho(f"Deleted session '{resolved_name}'.", fg=typer.colors.GREEN)
    else:
        typer.echo(f"No session named '{resolved_name}' found.")
