"""Job execution for the implicit-lifecycle job config.

``run_all`` is the primary path: algorithms are grouped by graph (by first
appearance in the `algorithms` list, keeping each graph's own algorithms in
their given order), and each graph is handled as one isolated unit of work -
projected, run, written back (if configured), then dropped - before moving on
to the next graph. The caller never declares project/drop steps by hand.

The per-step commands (``project`` / ``algorithms`` / ``writeback`` / ``drop``)
exist for k8s pipelines where each step is a separate pod: they reconnect to
the same session and see graphs by name in the server-side catalog, so they
can't do the same per-graph isolation within a single process. Instead they're
coarse-grained: project every graph upfront, then run (optionally filtered)
algorithms, then write back everything, then drop everything.

Every phase prints a clearly labeled section and a live line per unit of work
via :class:`~gds_cli.session.report.JobReport`, and a size/timing
summary table once the work is done - see ``report.py``.
"""

from __future__ import annotations

from typing import Any, Optional, TypedDict

from gds_cli.session.algorithms import run_algorithm
from gds_cli.session.config import AlgorithmConfig, JobConfig, ProjectionConfig, WritebackConfig
from gds_cli.session.report import JobReport
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.catalog.node_properties_endpoints import NodePropertiesWriteResult
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience


class AlgorithmResult(TypedDict):
    name: str
    graph_name: str
    result: Any


class WritebackResult(TypedDict):
    graph_name: str
    result: NodePropertiesWriteResult


class JobResult(TypedDict):
    algorithms: list[AlgorithmResult]
    writebacks: list[WritebackResult]


def _projection(cfg: JobConfig, graph_name: str) -> ProjectionConfig:
    for projection in cfg.projections:
        if projection.graph_name == graph_name:
            return projection
    raise ValueError(f"No projection named {graph_name!r} in the config.")


def _project_one(gds: AuraGraphDataScience, projection: ProjectionConfig, overwrite: bool, report: JobReport) -> Graph:
    if overwrite:
        report.note(f"dropping existing graph '{projection.graph_name}' if present (--overwrite-graph)")
        gds.graph.drop(projection.graph_name, fail_if_missing=False)
    undirected = projection.undirected_relationship_types or None
    with report.step(f"Projecting graph '{projection.graph_name}'") as elapsed:
        if projection.is_native:
            result = gds.graph.project.native(
                projection.graph_name,
                node_label_filter=projection.node_labels,
                relationship_type_filter=projection.relationship_types,
                node_properties=projection.node_properties or None,
                relationship_properties=projection.relationship_properties or None,
                undirected_relationship_types=undirected,
            )
        else:
            assert projection.query is not None  # not is_native => a remote query is present
            result = gds.graph.project.cypher(
                projection.graph_name,
                projection.query,
                undirected_relationship_types=undirected,
            )
        graph = result.graph
        node_count = graph.node_count()
        relationship_count = graph.relationship_count()
        report.note(f"{node_count:,} nodes, {relationship_count:,} relationships")
    report.record_projection(projection.graph_name, elapsed.seconds, node_count, relationship_count)
    return graph


def _run_one_algorithm(
    gds: AuraGraphDataScience, graph: Graph, algo: AlgorithmConfig, report: JobReport
) -> AlgorithmResult:
    target = algo.mutate_property if algo.mode == "mutate" else algo.write_property
    with report.step(f"Running algorithm '{algo.name}' ({algo.mode} -> {target})") as elapsed:
        result = run_algorithm(gds, graph, algo)
    report.record_algorithm(algo.graph_name, algo.name, elapsed.seconds)
    return {"name": algo.name, "graph_name": algo.graph_name, "result": result}


def _write_back_one(gds: AuraGraphDataScience, graph: Graph, wb: WritebackConfig, report: JobReport) -> WritebackResult:
    with report.step(f"Writing back {wb.node_properties} for graph '{wb.graph_name}'") as elapsed:
        write_result = gds.graph.node_properties.write(graph, wb.node_properties)
    report.record_writeback(wb.graph_name, elapsed.seconds)
    return {"graph_name": wb.graph_name, "result": write_result}


def _drop_one(gds: AuraGraphDataScience, graph_name: str, report: JobReport) -> None:
    with report.step(f"Dropping graph '{graph_name}'") as elapsed:
        gds.graph.drop(graph_name, fail_if_missing=False)
    report.record_drop(graph_name, elapsed.seconds)


def _group_by_graph(cfg: JobConfig) -> list[tuple[str, list[AlgorithmConfig]]]:
    """Group algorithms by graph, keyed by each graph's first appearance in the list."""
    groups: dict[str, list[AlgorithmConfig]] = {}
    for algo in cfg.algorithms:
        groups.setdefault(algo.graph_name, []).append(algo)
    return list(groups.items())


def run_all(
    gds: AuraGraphDataScience,
    cfg: JobConfig,
    overwrite_graph: bool = False,
    report: Optional[JobReport] = None,
) -> JobResult:
    """Run every graph as an isolated unit of work: project, run its algorithms in order,
    write back (if configured), then drop - before moving on to the next graph.

    ``overwrite_graph=True`` drops any existing same-named graph before projecting it.
    """
    report = report or JobReport()
    writeback_by_graph = {wb.graph_name: wb for wb in cfg.writebacks}
    algo_results: list[AlgorithmResult] = []
    writeback_results: list[WritebackResult] = []

    for graph_name, algos in _group_by_graph(cfg):
        report.graph_section(graph_name)
        graph = _project_one(gds, _projection(cfg, graph_name), overwrite=overwrite_graph, report=report)

        for algo in algos:
            algo_results.append(_run_one_algorithm(gds, graph, algo, report))

        wb = writeback_by_graph.get(graph_name)
        if wb is not None:
            writeback_results.append(_write_back_one(gds, graph, wb, report))

        _drop_one(gds, graph_name, report)

    report.summary()
    return {"algorithms": algo_results, "writebacks": writeback_results}


def project_all(
    gds: AuraGraphDataScience, cfg: JobConfig, overwrite: bool = False, report: Optional[JobReport] = None
) -> list[Graph]:
    """Step 1: project every graph declared in `projections`."""
    report = report or JobReport()
    report.section("Projecting graphs")
    graphs = [_project_one(gds, projection, overwrite=overwrite, report=report) for projection in cfg.projections]
    report.summary()
    return graphs


def get_graph(gds: AuraGraphDataScience, graph_name: str) -> Graph:
    """Look up an already-projected graph by name (for standalone later steps)."""
    return gds.graph.get(graph_name)


def run_algorithms(
    gds: AuraGraphDataScience, cfg: JobConfig, only: Optional[str] = None, report: Optional[JobReport] = None
) -> list[AlgorithmResult]:
    """Step 2: run the ordered list of algorithms, assuming their graphs are already projected.

    ``only`` restricts the run to the single named algorithm.
    """
    report = report or JobReport()
    report.section("Running algorithms")
    results: list[AlgorithmResult] = []
    for algo in cfg.algorithms:
        if only is not None and algo.name != only:
            continue
        graph = get_graph(gds, algo.graph_name)
        results.append(_run_one_algorithm(gds, graph, algo, report))
    if only is not None and not results:
        raise ValueError(f"No algorithm named {only!r} in the config.")
    report.summary()
    return results


def run_writebacks(
    gds: AuraGraphDataScience, cfg: JobConfig, report: Optional[JobReport] = None
) -> list[WritebackResult]:
    """Step 3: write every configured graph's mutated node properties back to the DB."""
    report = report or JobReport()
    report.section("Writing back")
    results: list[WritebackResult] = []
    for wb in cfg.writebacks:
        graph = get_graph(gds, wb.graph_name)
        results.append(_write_back_one(gds, graph, wb, report))
    report.summary()
    return results


def drop_all(gds: AuraGraphDataScience, cfg: JobConfig, report: Optional[JobReport] = None) -> None:
    """Drop every projected graph from the session catalog, keeping the session for reuse.

    Safe if a graph is already missing.
    """
    report = report or JobReport()
    report.section("Dropping graphs")
    for projection in cfg.projections:
        _drop_one(gds, projection.graph_name, report)
    report.summary()
