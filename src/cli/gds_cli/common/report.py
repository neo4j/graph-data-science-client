"""Shared rich-console step reporting for the ``gds`` CLI.

Renders a clearly labeled section per phase and a live line per unit of work with
its elapsed time, plus a final summary line. Used by ``gds run`` (via the
:class:`~gds_cli.session.report.JobReport` subclass, which adds a size/timing
table) and by ``gds database upload`` directly, so both read the same way.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Iterator

from rich.console import Console

from gds_cli.common import CONSOLE_WIDTH


class Elapsed:
    """Mutable holder for a step's duration, filled in when its `with` block exits."""

    seconds: float = 0.0


class StepReporter:
    """Live progress printer: labeled sections and per-step lines with elapsed time.

    ``quiet=True`` suppresses all printed output while still tracking wall-clock
    time - use it to honor a ``--no-progress-bar``-style flag without threading
    conditionals through every call site.
    """

    def __init__(self, quiet: bool = False) -> None:
        self._console = Console(width=CONSOLE_WIDTH, quiet=quiet)
        self._start = time.monotonic()

    def section(self, title: str) -> None:
        """Print a section header for a phase or unit of work."""
        self._console.print()
        self._console.rule(f"[bold cyan]{title}[/bold cyan]")

    @contextmanager
    def step(self, message: str) -> Iterator[Elapsed]:
        """Print ``message ...``, then a done/failed line with elapsed time."""
        self._console.print()
        self._console.print(f"[cyan]->[/cyan] {message} ...")
        start = time.monotonic()
        elapsed = Elapsed()
        try:
            yield elapsed
        except Exception:
            elapsed.seconds = time.monotonic() - start
            self._console.print(f"  [red]x[/red] failed after {elapsed.seconds:.2f}s")
            raise
        else:
            elapsed.seconds = time.monotonic() - start
            self._console.print(f"  [green]✓[/green] done in {elapsed.seconds:.2f}s")

    def note(self, message: str) -> None:
        self._console.print(f"  [blue]i[/blue] [dim]{message}[/dim]")

    def total_seconds(self) -> float:
        return time.monotonic() - self._start

    def done(self, message: str) -> None:
        """Print a final summary rule + message with the total wall-clock time."""
        self._console.print()
        self._console.rule("[bold]Summary[/bold]")
        self._console.print(f"{message} ({self.total_seconds():.2f}s)")
