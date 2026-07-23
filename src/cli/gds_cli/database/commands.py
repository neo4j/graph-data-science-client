"""``gds database`` — generate test graphs, upload them to Aura Neo4j, and read
them back to inspect what a GDS job wrote.

Connection details come from the unified env set (see
:mod:`gds_cli.common.env`). A ``.env`` in the working directory is
loaded automatically; pass --env-file for another dotenv file. Real environment
variables always take precedence over dotenv files.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Optional

import typer

from gds_cli.database.output import OutputFormat

if TYPE_CHECKING:
    from gds_cli.common.env import DatabaseConfig
    from gds_cli.database.db import DatabaseClient
    from gds_cli.database.graph import Graph

app = typer.Typer(
    help="Generate test graphs, upload them to Aura Neo4j, and read them back. Alias: `gds db`.",
    no_args_is_help=True,
)


def _print_connection(config: DatabaseConfig) -> None:
    """Print which database we're about to talk to (to stderr, so it never
    pollutes stdout output like ``fetch -o json``)."""
    from rich.console import Console

    from gds_cli.common import CONSOLE_WIDTH

    console = Console(stderr=True, width=CONSOLE_WIDTH)
    console.rule("[bold cyan]Database[/bold cyan]")
    console.print(f"  [blue]i[/blue] URI:      [bold]{config.uri}[/bold]")
    console.print(f"  [blue]i[/blue] Database: [bold]{config.database}[/bold]")
    console.print(f"  [blue]i[/blue] User:     [bold]{config.username}[/bold]")


def _client(env_file: Optional[str]) -> DatabaseClient:
    from gds_cli.common.env import load_env
    from gds_cli.database.db import DatabaseClient

    load_env(env_file)
    client = DatabaseClient.from_env()
    _print_connection(client.db_config)
    return client


ENV_FILE_OPT = typer.Option(
    None,
    "--env-file",
    help="Extra dotenv file to read (a .env in the working dir loads automatically; real env vars win).",
)

FILE_OPT = typer.Option(
    ...,
    "--file",
    "-f",
    help=(
        "Graph file(s) or a directory. Repeat -f for several, or pass a folder to upload every "
        "*.json/*.yaml graph in it. Each file is JSON construct format or a random-graph spec "
        "(kind: random, JSON/YAML)."
    ),
)

_GRAPH_SUFFIXES = (".json", ".yaml", ".yml")


def _resolve_files(paths: list[str]) -> list[Path]:
    """Expand the given file/directory arguments into a de-duplicated, ordered list
    of graph files. A directory contributes its *.json/*.yaml members (sorted)."""
    resolved: list[Path] = []
    seen: set[Path] = set()
    for raw in paths:
        path = Path(raw).expanduser()
        if path.is_dir():
            members = sorted(f for f in path.iterdir() if f.suffix.lower() in _GRAPH_SUFFIXES)
            if not members:
                typer.secho(
                    f"Error: no graph files (*.json/*.yaml) in directory '{path}'.", fg=typer.colors.RED, err=True
                )
                raise typer.Exit(1)
            candidates = members
        else:
            candidates = [path]
        for candidate in candidates:
            key = candidate.resolve()
            if key not in seen:
                seen.add(key)
                resolved.append(candidate)
    return resolved


def _build(file: str) -> Graph:
    from gds_cli.database.construct import graph_from_file

    try:
        return graph_from_file(file)
    except (ValueError, KeyError, OSError) as exc:
        typer.secho(f"Error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc


def _counts(graph: Graph) -> tuple[int, int]:
    """Total (nodes, relationships) across all labels/types in the graph."""
    nodes = sum(len(df) for df in graph.node_dfs.values())
    rels = sum(len(df) for df in graph.rel_dfs.values())
    return nodes, rels


@app.command()
def upload(
    files: list[str] = FILE_OPT,
    overwrite: bool = typer.Option(False, "--overwrite", help="Replace existing test data with the same labels."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Build and summarize only; do not write to the database."),
    progress_bar: bool = typer.Option(
        True, "--progress-bar/--no-progress-bar", help="Show upload progress bars. On by default."
    ),
    env_file: Optional[str] = ENV_FILE_OPT,
) -> None:
    """Upload one or more graphs (--file/-f, repeatable, or a directory): JSON construct format or a random-graph spec.

    Multiple files (or a folder) are handled one at a time, reported file-by-file
    like ``gds run`` reports its jobs.
    """
    from gds_cli.common.report import StepReporter
    from gds_cli.database.output import print_summary

    report = StepReporter()
    # Build every graph first, so a bad file fails before we touch the database.
    graphs = [(path, _build(str(path))) for path in _resolve_files(files)]

    if dry_run:
        for path, graph in graphs:
            report.section(f"Graph: {path.name}")
            print_summary(graph)
        typer.echo("Dry run: nothing written.")
        return

    client = _client(env_file)
    for path, graph in graphs:
        report.section(f"Graph: {path.name}")
        file_overwrite = overwrite
        if not file_overwrite and client.exists(graph):
            # Existing same-labelled test data would otherwise cause upload to fail;
            # ask instead of forcing the user to re-run with --overwrite.
            typer.confirm(f"Test data with '{path.name}' labels already exists. Overwrite it?", abort=True)
            file_overwrite = True
        nodes, rels = _counts(graph)
        with report.step(f"Uploading '{path}'"):
            report.note(f"{nodes:,} nodes, {rels:,} relationships")
            client.upload(graph, overwrite=file_overwrite, show_progress=progress_bar)
    report.done(f"Uploaded {len(graphs)} graph(s)")


LABEL_OPT = typer.Option(None, "--label", help="Only include nodes with this label.")


def _fetch(env_file: Optional[str], label: Optional[str]) -> Graph:
    client = _client(env_file)
    return client.fetch(node_labels=[label] if label else None)


@app.command()
def summary(
    label: Optional[str] = LABEL_OPT,
    env_file: Optional[str] = ENV_FILE_OPT,
) -> None:
    """Download test data and print a compact summary (counts + property names)."""
    from gds_cli.database.output import print_summary

    print_summary(_fetch(env_file, label))


@app.command()
def fetch(
    label: Optional[str] = LABEL_OPT,
    output: OutputFormat = typer.Option(
        OutputFormat.table, "--output", "-o", help="Output format: table (default) or json."
    ),
    limit: str = typer.Option("10", "--limit", "-n", help="Max rows per table, or 'all'."),
    env_file: Optional[str] = ENV_FILE_OPT,
) -> None:
    """Download test data and print full node/relationship tables (or construct JSON).

    The ``json`` format is the construct format ``upload --file`` accepts, so a
    graph round-trips: ``fetch -o json > g.json`` then ``upload -f g.json``.
    """
    graph = _fetch(env_file, label)
    if output is OutputFormat.json:
        from gds_cli.database.output import print_json

        print_json(graph)
        return
    if limit.lower() == "all":
        row_limit: Optional[int] = None
    else:
        try:
            row_limit = int(limit)
        except ValueError as exc:
            raise typer.BadParameter("--limit must be an integer or 'all'.") from exc

    from gds_cli.database.output import print_tables

    print_tables(graph, row_limit)


@app.command()
def delete(
    label: Optional[str] = typer.Option(None, "--label", help="Delete only nodes with this label."),
    all: bool = typer.Option(False, "--all", help="Delete all test (Dev-labelled) data."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Report what would be deleted without deleting."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt (for scripts)."),
    env_file: Optional[str] = ENV_FILE_OPT,
) -> None:
    """Delete uploaded test data from the database. Prompts for confirmation first."""
    if not all and not label:
        raise typer.BadParameter("Pass --all or --label <LABEL>.")
    client = _client(env_file)
    target = "all Dev-labelled test data" if all else f"test data with label '{label}'"
    # Count first (never deletes) so we can report the scope and confirm before touching data.
    preview = client.delete(all=True, dry_run=True) if all else client.delete(label, dry_run=True)
    if dry_run:
        typer.echo(f"Would delete {preview.nodes} nodes and {preview.relationships} relationships ({target}).")
        return
    if preview.nodes == 0:
        typer.echo(f"Nothing to delete ({target}).")
        return
    if not yes:
        typer.confirm(
            f"Delete {preview.nodes} nodes and {preview.relationships} relationships ({target})?",
            abort=True,
        )
    stats = client.delete(all=True) if all else client.delete(label)
    typer.echo(f"Deleted {stats.nodes} nodes and {stats.relationships} relationships ({target}).")
