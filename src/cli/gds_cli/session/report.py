"""Rich console reporting for `gds session` job execution.

Prints a clearly labeled section per phase (connecting, projecting, running
algorithms, writing back, dropping), a live line per unit of work with its
graph name and timing, and a final summary table of data size and execution
time once the job finishes.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Iterator, Optional

from rich.console import Console
from rich.table import Table

from gds_cli.common import CONSOLE_WIDTH


class Elapsed:
    """Mutable holder for a step's duration, filled in when its `with` block exits."""

    seconds: float = 0.0


@dataclass
class GraphStats:
    """Accumulated size/timing info for one graph across a job run."""

    graph_name: str
    node_count: Optional[int] = None
    relationship_count: Optional[int] = None
    project_seconds: float = 0.0
    # One (name, seconds) entry per algorithm *run*, in execution order - a list,
    # not a name-keyed dict, so the same algorithm run more than once on a graph
    # (e.g. pageRank with two parameter sets) keeps every run's timing.
    algorithm_timings: list[tuple[str, float]] = field(default_factory=list)
    writeback_seconds: float = 0.0
    drop_seconds: float = 0.0

    @property
    def total_seconds(self) -> float:
        algorithm_seconds = sum(seconds for _, seconds in self.algorithm_timings)
        return self.project_seconds + algorithm_seconds + self.writeback_seconds + self.drop_seconds


class JobReport:
    """Live progress printer + timing/size accumulator for one CLI invocation.

    ``quiet=True`` suppresses all printed output (sections, steps, summary)
    while still accumulating timing/size stats internally - use it to honor a
    ``--no-progress-bar`` CLI flag without threading conditionals through every
    call site.
    """

    def __init__(self, quiet: bool = False) -> None:
        self._console = Console(width=CONSOLE_WIDTH, quiet=quiet)
        self._graphs: dict[str, GraphStats] = {}
        self._start = time.monotonic()

    def _stats(self, graph_name: str) -> GraphStats:
        return self._graphs.setdefault(graph_name, GraphStats(graph_name=graph_name))

    def section(self, title: str) -> None:
        """Print a section header for a whole phase (e.g. "Projecting graphs")."""
        self._console.print()
        self._console.rule(f"[bold cyan]{title}[/bold cyan]")

    def graph_section(self, graph_name: str) -> None:
        """Print a section header for one graph's isolated block of work."""
        self._console.print()
        self._console.rule(f"[bold cyan]Graph: {graph_name}[/bold cyan]")

    @contextmanager
    def step(self, message: str) -> Iterator[Elapsed]:
        """Print ``message ...``, then a done/failed line with elapsed time.

        Usage::

            with report.step("Projecting graph 'social'") as elapsed:
                ...
            report.record_projection("social", elapsed.seconds, ...)
        """
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

    def record_projection(self, graph_name: str, seconds: float, node_count: int, relationship_count: int) -> None:
        """Record size/timing stats for the summary table.

        Call this after the `step()` block exits, once `elapsed.seconds` is
        final. It does not print anything itself - print the node/relationship
        counts with `note()` inside the step, before "done", if wanted live.
        """
        stats = self._stats(graph_name)
        stats.project_seconds += seconds
        stats.node_count = node_count
        stats.relationship_count = relationship_count

    def record_algorithm(self, graph_name: str, algorithm_name: str, seconds: float) -> None:
        self._stats(graph_name).algorithm_timings.append((algorithm_name, seconds))

    def record_writeback(self, graph_name: str, seconds: float) -> None:
        self._stats(graph_name).writeback_seconds += seconds

    def record_drop(self, graph_name: str, seconds: float) -> None:
        self._stats(graph_name).drop_seconds += seconds

    def summary(self) -> None:
        """Print the final size/timing table plus total wall-clock time."""
        total = time.monotonic() - self._start

        table = Table(show_header=True, header_style="bold")
        table.add_column("Graph")
        table.add_column("Algorithms")
        table.add_column("Nodes", justify="right")
        table.add_column("Rels", justify="right")
        table.add_column("Projection", justify="right")
        table.add_column("Writebacks", justify="right")
        table.add_column("Total", justify="right")

        for stats in self._graphs.values():
            algos = (
                "\n".join(f"{name} ({secs:.2f}s)" for name, secs in stats.algorithm_timings)
                if stats.algorithm_timings
                else "-"
            )
            nodes = f"{stats.node_count:,}" if stats.node_count is not None else "-"
            rels = f"{stats.relationship_count:,}" if stats.relationship_count is not None else "-"
            projection = f"{stats.project_seconds:.2f}s" if stats.project_seconds else "-"
            writebacks = f"{stats.writeback_seconds:.2f}s" if stats.writeback_seconds else "-"
            table.add_row(stats.graph_name, algos, nodes, rels, projection, writebacks, f"{stats.total_seconds:.2f}s")

        self._console.print()
        self._console.rule("[bold]Summary[/bold]")
        self._console.print(table)
        self._console.print(f"Total time: {total:.2f}s")
