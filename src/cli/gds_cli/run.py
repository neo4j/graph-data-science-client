"""``gds run`` — run a standardized GDS job config against a managed Aura GDS session.

Feed a job config (a ``session`` block + a list of ``jobs``) and run the whole
pipeline: each job's graph is projected, its ``compute`` steps run (a result is
materialized into the graph when a later compute consumes it as a feature -
auto-derived, with an optional ``mutate`` override), its ``write`` results
persisted to the database, then the graph dropped - before the next job starts.
See :mod:`gds_cli.session.steps` for the execution engine and
:mod:`gds_cli.session.config` for the config schema.

The config is a file (``--file``/``-f``) or, if omitted, a YAML document in the
``$GDS_JOB_CONFIG`` env var - the latter lets a single k8s Job carry its config
inline instead of needing a paired ConfigMap + volume mount. Credentials come
from the unified env set (see :mod:`gds_cli.common.env`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import typer

from gds_cli.session.commands import ENV_FILE_OPT, PROGRESS_BAR_OPT, _connect, _load

if TYPE_CHECKING:
    from gds_cli.session.config import JobsConfig
    from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience


def _resolve_overwrite(
    gds: AuraGraphDataScience,
    cfg: JobsConfig,
    session_name: str,
    overwrite_graph: bool,
    assume_yes: bool = False,
) -> bool:
    """Decide whether to drop-before-project, prompting if graphs are already projected.

    Returns the effective overwrite flag. If ``--overwrite-graph`` was given, drops
    unconditionally. Otherwise, if any job's graph is already in the session catalog
    (e.g. left over from an interrupted run), prompt the user to drop and re-project;
    aborting if they decline. ``assume_yes`` (``--yes``) answers that prompt
    affirmatively without asking, for non-interactive runs (e.g. a k8s Job).
    """
    from gds_cli.session.steps import job_graph_name

    if overwrite_graph:
        return True
    existing = [job_graph_name(i) for i in range(len(cfg.jobs)) if gds.graph.exists(job_graph_name(i))]
    if not existing:
        return False
    typer.secho(
        f"Graph(s) {', '.join(existing)} are already projected in session '{session_name}' "
        "(e.g. left over from an interrupted run).",
        fg=typer.colors.YELLOW,
    )
    if assume_yes:
        typer.secho("Dropping and re-projecting them (--yes).", fg=typer.colors.YELLOW)
        return True
    if typer.confirm("Drop and re-project them?", default=False):
        return True
    typer.secho("Aborted. Re-run with --overwrite-graph or --yes to drop automatically.", fg=typer.colors.RED)
    raise typer.Exit(code=1)


_CONFIG_SUFFIXES = (".yaml", ".yml")

FILES_OPT = typer.Option(
    None,
    "--file",
    "-f",
    help=(
        "Job config(s): a YAML file or a directory of them; repeat -f for several "
        "(files and directories may be mixed). If omitted, reads YAML from $GDS_JOB_CONFIG."
    ),
)


def _load_configs(files: Optional[list[str]], env_file: Optional[str]) -> list[tuple[str, str, JobsConfig]]:
    """Resolve ``--file`` args into an ordered list of ``(name, base_dir, config)`` tuples.

    No ``-f`` reads one config from ``$GDS_JOB_CONFIG``. Otherwise each ``-f`` is a
    file or a **directory** (contributing its sorted ``*.yaml``/``*.yml`` members);
    all are expanded in order and de-duplicated, each run independently. ``base_dir``
    (used to resolve a config's ``construct`` file and standalone ``outputFile`` paths)
    is that config file's directory. ``name`` is the config's file name, shown as a
    header when more than one config runs.
    """
    from pathlib import Path

    if not files:
        return [("$GDS_JOB_CONFIG", ".", _load(None, env_file))]

    resolved: list[Path] = []
    seen: set[Path] = set()
    for raw in files:
        path = Path(raw).expanduser()
        if path.is_dir():
            members = sorted(f for f in path.iterdir() if f.suffix.lower() in _CONFIG_SUFFIXES)
            if not members:
                typer.secho(f"Error: no job configs (*.yaml) in directory '{path}'.", fg=typer.colors.RED, err=True)
                raise typer.Exit(1)
            candidates = members
        else:
            candidates = [path]
        for candidate in candidates:
            key = candidate.resolve()
            if key not in seen:
                seen.add(key)
                resolved.append(candidate)

    # load_env re-runs harmlessly per file (real env always wins).
    return [(p.name, str(p.parent), _load(str(p), env_file)) for p in resolved]


def _run_one_config(
    name: str,
    base_dir: str,
    cfg: JobsConfig,
    session_name: Optional[str],
    overwrite_graph: bool,
    progress_bar: bool,
    assume_yes: bool = False,
) -> None:
    """Run a single config as a self-contained unit: connect its own session, execute,
    then delete the session unless the user named one to keep.

    Each config owns its session end to end, using its own ``session:`` block
    (memory/ttl and standalone-vs-attached), so a directory run behaves exactly like
    invoking ``gds run -f <file>`` once per file.
    """
    from uuid import uuid4

    from gds_cli.session.report import JobReport
    from gds_cli.session.session_ops import delete as delete_session_op
    from gds_cli.session.steps import run_all

    # Name precedence: --session-name, then the config's `session.name`, else a fresh
    # throwaway. A named session (either source) is kept; an anonymous one is deleted.
    chosen_name = session_name or cfg.session.name
    keep_session = chosen_name is not None
    resolved_name = chosen_name or f"cli-{uuid4()}"

    report = JobReport()
    gds = _connect(cfg.session, resolved_name, report, progress_bar=progress_bar)
    try:
        eff_overwrite = _resolve_overwrite(gds, cfg, resolved_name, overwrite_graph, assume_yes=assume_yes)
        run_all(gds, cfg, overwrite_graph=eff_overwrite, report=report, base_dir=base_dir)
    finally:
        if not keep_session:
            delete_session_op(resolved_name)
            typer.echo(f"Deleted throwaway session '{resolved_name}'.")
    if keep_session:
        typer.echo(f"Session '{resolved_name}' kept (reuse with --session-name {resolved_name}).")


def run(
    files: Optional[list[str]] = FILES_OPT,
    session_name: Optional[str] = typer.Option(
        None,
        "--session-name",
        envvar="GDS_RUNNER_SESSION_NAME",
        help=(
            "Reuse or create a session with this exact name and KEEP it after the run "
            "(e.g. to reuse a warm session across runs). Omit to create a fresh, uniquely-named "
            "session that is deleted once the run completes."
        ),
    ),
    overwrite_graph: bool = typer.Option(
        False, "--overwrite-graph", help="Drop an existing same-named graph before projecting."
    ),
    assume_yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        envvar="GDS_RUNNER_ASSUME_YES",
        help=(
            "Assume 'yes' for confirmation prompts (non-interactive runs, e.g. a k8s Job driven by "
            "$GDS_JOB_CONFIG): if a session has leftover graphs, drop and re-project them without asking."
        ),
    ),
    progress_bar: bool = PROGRESS_BAR_OPT,
    env_file: Optional[str] = ENV_FILE_OPT,
) -> None:
    """Run the config(s): each job's graph projected, computed, written, and dropped in turn.

    ``--file``/``-f`` takes a job config or a **directory** of them and may be repeated
    (``-f a.yaml -f b.yaml``, files and directories mixed). Every resolved config runs
    independently - exactly as if ``gds run -f <file>`` were invoked once per file: each
    creates (and, unless ``--session-name`` keeps it, deletes) its own session from its
    own ``session:`` block.

    For unattended runs (a k8s Job reading its config from ``$GDS_JOB_CONFIG``), pass
    ``--overwrite-graph`` to always drop a pre-existing graph, or ``--yes`` to auto-confirm
    the drop-and-re-project prompt for a reused session - neither blocks on input.
    """
    configs = _load_configs(files, env_file)
    for name, base_dir, cfg in configs:
        if len(configs) > 1:
            _config_header(name)
        _run_one_config(name, base_dir, cfg, session_name, overwrite_graph, progress_bar, assume_yes=assume_yes)
    typer.secho("Job complete.", fg=typer.colors.GREEN)


def _config_header(name: str) -> None:
    """Print a centered, double-lined rule naming the config about to run."""
    from rich.console import Console

    from gds_cli.common import CONSOLE_WIDTH

    console = Console(width=CONSOLE_WIDTH)
    console.print()
    # `═` gives a double line; rule() centers the title by default.
    console.rule(f"[bold cyan]{name}[/bold cyan]", characters="═")
