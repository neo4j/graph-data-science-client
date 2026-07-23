"""Root ``gds`` command: ``gds run``, ``gds database ...``, and ``gds sessions ...``."""

from __future__ import annotations

import warnings
from typing import Any

try:
    import typer
except ImportError as exc:
    raise SystemExit(
        "The `gds` CLI requires dependencies that are not installed.\n"
        "Install the CLI with: uv tool install --editable ./src/cli\n"
        "(or from git: uv tool install "
        '"git+https://github.com/neo4j/graph-data-science-client.git#subdirectory=src/cli")'
    ) from exc

from typer.core import TyperGroup

from gds_cli.database.commands import app as database_app
from gds_cli.run import run as run_command
from gds_cli.session.commands import app as sessions_app

# The underlying SDK warns (e.g. FastPath's "preview feature" notice) via
# warnings.warn, which writes straight to stderr and bypasses our rich console
# reporting. Silence those here so the CLI's own report stays the one source
# of truth for what's happening.
warnings.filterwarnings("ignore", category=UserWarning)

_EXPERIMENTAL_HEAD = "EXPERIMENTAL, development-only tool - not part of the public GDS API."
_EXPERIMENTAL_TAIL = "May change or break without notice!"
_EXPERIMENTAL_NOTICE = f"{_EXPERIMENTAL_HEAD} {_EXPERIMENTAL_TAIL}"


class _AliasGroup(TyperGroup):
    """Command group that maps alias names to their canonical command.

    Aliases are resolved at dispatch (``get_command``) but are NOT listed as their
    own entries in ``--help``, so an alias reads as one command with a second name
    rather than a duplicated group.
    """

    _ALIASES = {"db": "database"}

    def get_command(self, ctx: Any, cmd_name: str) -> Any:
        return super().get_command(ctx, self._ALIASES.get(cmd_name, cmd_name))


app = typer.Typer(
    cls=_AliasGroup,
    help=f"Run GDS jobs and manage GDS test databases and Aura GDS sessions.\n\n{_EXPERIMENTAL_NOTICE}",
    no_args_is_help=True,
)


@app.callback()
def _main() -> None:
    """Print the experimental-tool banner before running any command.

    Runs on command execution (not on ``--help``), so every ``gds ...`` invocation
    reminds the user this is a dev tool. Goes to stderr to keep stdout clean.
    """
    # A flagged head line, then the warning hand-indented toward the middle, in red.
    typer.secho(f"⚠  gds: {_EXPERIMENTAL_HEAD}", fg=typer.colors.YELLOW, err=True)
    typer.secho(f"⚠  {_EXPERIMENTAL_TAIL}", fg=typer.colors.RED, bold=True, err=True)


app.command(name="run")(run_command)
# `db` is an alias for `database`, resolved by _AliasGroup (not a second entry).
app.add_typer(database_app, name="database")
app.add_typer(sessions_app, name="sessions")
