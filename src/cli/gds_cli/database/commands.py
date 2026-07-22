"""``gds database`` — generate test graphs, upload them to Aura Neo4j, and read
them back to inspect what a GDS job wrote.

Connection details come from the unified env set (see
:mod:`gds_cli.common.env`). A ``.env`` in the working directory is
loaded automatically; pass --env-file for another dotenv file. Real environment
variables always take precedence over dotenv files.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import typer

from gds_cli.database.output import OutputFormat

if TYPE_CHECKING:
    from gds_cli.common.env import DatabaseConfig
    from gds_cli.database.db import DatabaseClient
    from gds_cli.database.graph import Graph

app = typer.Typer(
    help="Generate test graphs, upload them to Aura Neo4j, and read them back.",
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
    help="Graph file: JSON construct format, or a random-graph spec (kind: random, JSON/YAML).",
)


def _build(file: str) -> Graph:
    from gds_cli.database.construct import graph_from_file

    try:
        return graph_from_file(file)
    except (ValueError, KeyError, OSError) as exc:
        typer.secho(f"Error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(1) from exc


@app.command()
def upload(
    file: str = FILE_OPT,
    overwrite: bool = typer.Option(False, "--overwrite", help="Replace existing test data with the same labels."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Build and summarize only; do not write to the database."),
    progress_bar: bool = typer.Option(
        True, "--progress-bar/--no-progress-bar", help="Show upload progress bars. On by default."
    ),
    env_file: Optional[str] = ENV_FILE_OPT,
) -> None:
    """Upload a graph from a file (--file/-f): JSON construct format, or a random-graph spec."""
    from gds_cli.database.output import print_summary

    graph = _build(file)
    print_summary(graph)
    if dry_run:
        typer.echo("Dry run: nothing written.")
        return
    client = _client(env_file)
    if not overwrite and client.exists(graph):
        # Existing same-labelled test data would otherwise cause upload to fail;
        # ask instead of forcing the user to re-run with --overwrite.
        typer.confirm("Test data with these labels already exists. Overwrite it?", abort=True)
        overwrite = True
    client.upload(graph, overwrite=overwrite, show_progress=progress_bar)
    typer.echo(f"Uploaded file '{file}'.")


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
    env_file: Optional[str] = ENV_FILE_OPT,
) -> None:
    """Delete uploaded test data from the database."""
    if not all and not label:
        raise typer.BadParameter("Pass --all or --label <LABEL>.")
    client = _client(env_file)
    target = "all Dev-labelled test data" if all else f"test data with label '{label}'"
    stats = client.delete(all=True, dry_run=dry_run) if all else client.delete(label, dry_run=dry_run)
    verb = "Would delete" if dry_run else "Deleted"
    typer.echo(f"{verb} {stats.nodes} nodes and {stats.relationships} relationships ({target}).")
