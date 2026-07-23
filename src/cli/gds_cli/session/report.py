"""Rich console reporting for `gds session` job execution.

Prints a clearly labeled section per phase (connecting, projecting, running
algorithms, writing back, dropping), a live line per unit of work with its
graph name and timing, and a final summary table of data size and execution
time once the job finishes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from rich.table import Table

from gds_cli.common.report import Elapsed, StepReporter

__all__ = ["Elapsed", "GraphStats", "JobReport"]


@dataclass
class GraphStats:
    """Accumulated size/timing info for one graph across a job run."""

    graph_name: str
    node_count: Optional[int] = None
    relationship_count: Optional[int] = None
    project_seconds: float = 0.0
    # One (name, seconds) entry per algorithm *run* / writeback, in execution order -
    # lists, not name-keyed dicts, so the same algorithm/property handled more than
    # once on a graph keeps every run's timing.
    algorithm_timings: list[tuple[str, float]] = field(default_factory=list)
    writeback_timings: list[tuple[str, float]] = field(default_factory=list)
    drop_seconds: float = 0.0

    @property
    def total_seconds(self) -> float:
        algorithm_seconds = sum(seconds for _, seconds in self.algorithm_timings)
        writeback_seconds = sum(seconds for _, seconds in self.writeback_timings)
        return self.project_seconds + algorithm_seconds + writeback_seconds + self.drop_seconds


class JobReport(StepReporter):
    """``StepReporter`` plus per-graph timing/size accumulation for one job run.

    Adds a ``job_section`` header and a final size/timing summary table on top
    of the shared section/step/note rendering. ``quiet=True`` (inherited)
    suppresses output while still accumulating stats - used to honor
    ``--no-progress-bar``.
    """

    def __init__(self, quiet: bool = False) -> None:
        super().__init__(quiet=quiet)
        self._graphs: dict[str, GraphStats] = {}

    def _stats(self, graph_name: str) -> GraphStats:
        return self._graphs.setdefault(graph_name, GraphStats(graph_name=graph_name))

    def job_section(self, graph_name: str) -> None:
        """Print a section header for one job's isolated block of work."""
        self.section(f"Job: {graph_name}")

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

    def record_writeback(self, graph_name: str, node_property: str, seconds: float) -> None:
        self._stats(graph_name).writeback_timings.append((node_property, seconds))

    def record_drop(self, graph_name: str, seconds: float) -> None:
        self._stats(graph_name).drop_seconds += seconds

    def summary(self) -> None:
        """Print the final per-job size/timing table plus total wall-clock time.

        One row per job: node/rel counts first, then projection time, then a
        per-algorithm and per-writeback-property breakdown, and the job total.
        """
        total = self.total_seconds()

        # show_lines draws a rule between rows, i.e. a separator between each job.
        table = Table(show_header=True, header_style="bold", show_lines=True)
        table.add_column("Job")
        table.add_column("Graph")
        table.add_column("Projection", justify="right")
        table.add_column("Algorithms", justify="right")
        table.add_column("Writebacks", justify="right")
        table.add_column("Total", justify="right")

        for stats in self._graphs.values():
            nodes = f"{stats.node_count:,}" if stats.node_count is not None else "-"
            rels = f"{stats.relationship_count:,}" if stats.relationship_count is not None else "-"
            graph = f"Nodes: {nodes}\nRels: {rels}"
            projection = f"{stats.project_seconds:.2f}s" if stats.project_seconds else "-"
            algorithms = "\n".join(f"{name}: {secs:.2f}s" for name, secs in stats.algorithm_timings) or "-"
            writebacks = "\n".join(f"{prop}: {secs:.2f}s" for prop, secs in stats.writeback_timings) or "-"
            table.add_row(stats.graph_name, graph, projection, algorithms, writebacks, f"{stats.total_seconds:.2f}s")

        self._console.print()
        self._console.rule("[bold]Summary[/bold]")
        self._console.print(table)
        self._console.print(f"Total time: {total:.2f}s")
