"""Root ``gds`` command: ``gds database ...`` and ``gds session ...``."""

from __future__ import annotations

import warnings

try:
    import typer
except ImportError as exc:
    raise SystemExit(
        "The `gds` CLI requires dependencies that are not installed.\n"
        "Install the CLI with: uv tool install --editable ./src/cli\n"
        "(or from git: uv tool install "
        '"git+https://github.com/neo4j/graph-data-science-client.git#subdirectory=src/cli")'
    ) from exc

from gds_cli.database.commands import app as database_app
from gds_cli.session.commands import app as session_app

# The underlying SDK warns (e.g. FastPath's "preview feature" notice) via
# warnings.warn, which writes straight to stderr and bypasses our rich console
# reporting. Silence those here so the CLI's own report stays the one source
# of truth for what's happening.
warnings.filterwarnings("ignore", category=UserWarning)

_EXPERIMENTAL_NOTICE = (
    "EXPERIMENTAL, development-only tool - not part of the supported "
    "graphdatascience API; may change or break without notice."
)

app = typer.Typer(
    help=f"Manage GDS test databases and Aura GDS sessions.\n\n{_EXPERIMENTAL_NOTICE}",
    no_args_is_help=True,
)


@app.callback()
def _main() -> None:
    """Print the experimental-tool banner before running any command.

    Runs on command execution (not on ``--help``), so every ``gds ...`` invocation
    reminds the user this is a dev tool. Goes to stderr to keep stdout clean.
    """
    typer.secho(f"⚠  gds: {_EXPERIMENTAL_NOTICE}", fg=typer.colors.YELLOW, err=True)


app.add_typer(database_app, name="database")
app.add_typer(session_app, name="session")
