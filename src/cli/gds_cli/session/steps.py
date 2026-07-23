"""Job execution for the ``session`` + ``jobs`` config.

``run_all`` is the primary path: each job is handled as one isolated unit of
work - its graph projected, its ``compute`` steps run in order, its ``write``
steps persisted, then the graph dropped - before the next job starts. Each job
owns exactly one projection; the CLI generates an internal catalog name
(``job-<i>``) per job since the config carries no graph name.

Per job, the flow is compute -> (mutate) -> write:

* every ``compute`` starts the algorithm and returns a :class:`JobHandle`;
* if the produced ``resultProperty`` is in the job's materialization set
  (``JobSpec.mutated_properties`` - auto-derived from a later compute consuming it,
  plus any explicit ``mutate`` overrides), it is materialized into the in-session
  graph *immediately* (in compute order), so a later ``compute`` can consume it;
* a ``write`` of a mutated property is a graph-level writeback
  (``gds.graph.node_properties.write``); a ``write`` of a property that was
  computed but not mutated is written directly from the compute handle, skipping
  the mutate step to save session memory.

A **standalone** session (config has cloud/region) has no database: its graph is
built from a file (``project.type: construct``), write-back to a DB is impossible,
so ``write`` properties are instead streamed to each write's ``outputFile``
(resolved relative to the job config; ``.csv`` or ``.json`` by extension; properties
sharing a file are written together). They are all materialized first so they can
be streamed from the graph.

``run_all`` is invoked by the top-level ``gds run`` command (see
:mod:`gds_cli.run`).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional, TypedDict

from gds_cli.session.algorithms import compute_algorithm
from gds_cli.session.config import ComputeSpec, JobsConfig, JobSpec, ProjectSpec, WriteSpec
from gds_cli.session.report import JobReport
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience


class ComputeResult(TypedDict):
    algorithm: str
    graph_name: str
    result_property: str
    result: Any  # mutate summary dict, or None when computed-only (written directly)


class WriteResult(TypedDict):
    graph_name: str
    node_property: str
    result: Any


class JobResult(TypedDict):
    computes: list[ComputeResult]
    writes: list[WriteResult]


def job_graph_name(index: int) -> str:
    """Deterministic internal catalog name for the ``index``-th job (0-based)."""
    return f"job-{index}"


def _project_one(
    gds: AuraGraphDataScience,
    project: ProjectSpec,
    graph_name: str,
    overwrite: bool,
    report: JobReport,
    base_dir: str,
) -> Graph:
    if overwrite:
        report.note(f"dropping existing graph '{graph_name}' if present (--overwrite-graph)")
        gds.graph.drop(graph_name, fail_if_missing=False)
    undirected = project.undirected_relationship_types or None
    source = f"construct from '{project.file}'" if project.is_construct else project.type
    with report.step(f"Projecting graph '{graph_name}' ({source})") as elapsed:
        if project.is_construct:
            graph = _construct_from_file(gds, project, graph_name, undirected, report, base_dir)
        elif project.is_native:
            graph = gds.graph.project.native(
                graph_name,
                node_label_filter=project.node_labels,
                relationship_type_filter=project.relationship_types,
                node_properties=project.node_properties or None,
                relationship_properties=project.relationship_properties or None,
                undirected_relationship_types=undirected,
            ).graph
        else:
            assert project.query is not None  # cypher projection => a query is present (validated)
            graph = gds.graph.project.cypher(
                graph_name,
                project.query,
                undirected_relationship_types=undirected,
            ).graph
        node_count = graph.node_count()
        relationship_count = graph.relationship_count()
        report.note(f"{node_count:,} nodes, {relationship_count:,} relationships")
    report.record_projection(graph_name, elapsed.seconds, node_count, relationship_count)
    return graph


def _construct_from_file(
    gds: AuraGraphDataScience,
    project: ProjectSpec,
    graph_name: str,
    undirected: Optional[list[str]],
    report: JobReport,
    base_dir: str,
) -> Graph:
    """Build an in-session graph from a file via ``gds.graph.construct`` (no DB needed).

    ``project.file`` is resolved relative to ``base_dir`` (the job config's directory)
    unless it is absolute, so graph files can live next to the job that uses them.
    """
    from gds_cli.database.construct import graph_from_file, graph_to_construct_frames

    assert project.file is not None  # construct projection => a file is present (validated)
    file_path = Path(project.file).expanduser()
    if not file_path.is_absolute():
        file_path = Path(base_dir) / file_path
    nodes, relationships, dropped = graph_to_construct_frames(graph_from_file(file_path))
    if dropped:
        report.note(f"skipping string properties unsupported by graph.construct: {', '.join(dropped)}")
    graph: Graph = gds.graph.construct(graph_name, nodes, relationships, undirected_relationship_types=undirected)
    return graph


def _write_back_mutated(
    gds: AuraGraphDataScience, graph: Graph, write: WriteSpec, graph_name: str, report: JobReport
) -> WriteResult:
    with report.step(f"Writing back '{write.node_property}' -> '{write.target}' (from in-session graph)") as elapsed:
        result = gds.graph.node_properties.write(graph, {write.node_property: write.target})
    report.record_writeback(graph_name, write.node_property, elapsed.seconds)
    return {"graph_name": graph_name, "node_property": write.node_property, "result": result}


def _write_direct(handle: JobHandle, write: WriteSpec, graph_name: str, report: JobReport) -> WriteResult:
    """Write a computed-but-not-mutated property straight from its compute handle."""
    with report.step(f"Writing back '{write.node_property}' -> '{write.target}' (from result store)") as elapsed:
        # The compute job was already waited on in `_run_one_compute`, so its result
        # is in the session result store; `JobHandle.write` itself does not wait for
        # the compute job (unlike `mutate`/`stream`). Without that prior wait the DB
        # would report "No entry with job id ... in result store".
        write_handle = handle.write(write_properties={write.node_property: write.target})
        result = write_handle.result()
    report.record_writeback(graph_name, write.node_property, elapsed.seconds)
    return {"graph_name": graph_name, "node_property": write.node_property, "result": result}


def _drop_one(gds: AuraGraphDataScience, graph_name: str, report: JobReport) -> None:
    with report.step(f"Dropping graph '{graph_name}'") as elapsed:
        gds.graph.drop(graph_name, fail_if_missing=False)
    report.record_drop(graph_name, elapsed.seconds)


def _run_one_compute(
    gds: AuraGraphDataScience, graph: Graph, spec: ComputeSpec, graph_name: str, mutate: bool, report: JobReport
) -> tuple[JobHandle, ComputeResult]:
    verb = "compute + mutate" if mutate else "compute"
    with report.step(f"Running '{spec.algorithm}' ({verb} -> {spec.result_property})") as elapsed:
        handle = compute_algorithm(gds, graph, spec)
        # `compute()` only submits the job; wait here so this step reflects the
        # algorithm's real runtime (not just submission) for every path - otherwise
        # a compute-only property would appear "done" while still running, and its
        # runtime would be misattributed to the later write step (see _write_direct).
        handle.wait()
        mutate_result = handle.mutate(mutate_property=spec.result_property) if mutate else None
    report.record_algorithm(graph_name, spec.algorithm, elapsed.seconds)
    return handle, {
        "algorithm": spec.algorithm,
        "graph_name": graph_name,
        "result_property": spec.result_property,
        "result": mutate_result,
    }


def _stream_group(
    gds: AuraGraphDataScience,
    graph: Graph,
    writes: list[WriteSpec],
    output_file: str,
    graph_name: str,
    base_dir: str,
    report: JobReport,
) -> list[WriteResult]:
    """Stream one output file's worth of properties out of a standalone session.

    A standalone session has no DB to write back to, so the properties (materialized
    during compute) are streamed from the graph together and saved to ``output_file``
    (resolved relative to the job config's directory), columns renamed to their write
    targets. The file format is chosen by extension: ``.json`` mirrors the construct
    input file, wrapping the streamed rows under a ``computedNodeProperties`` section;
    anything else writes CSV.
    """
    properties = [write.node_property for write in writes]
    rename = {write.node_property: write.target for write in writes if write.target != write.node_property}
    path = Path(output_file).expanduser()
    if not path.is_absolute():
        path = Path(base_dir) / path
    with report.step(f"Streaming {properties} -> '{path}' (standalone, no database)") as elapsed:
        df = gds.graph.node_properties.stream(graph, properties)
        if rename:
            df = df.rename(columns=rename)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix.lower() == ".json":
            records = df.to_dict(orient="records")
            path.write_text(json.dumps({"computedNodeProperties": records}, indent=2) + "\n")
        else:
            df.to_csv(path, index=False)
    for write in writes:
        report.record_writeback(graph_name, write.node_property, elapsed.seconds)
    return [{"graph_name": graph_name, "node_property": write.node_property, "result": str(path)} for write in writes]


def _run_one_job(
    gds: AuraGraphDataScience,
    job: JobSpec,
    graph_name: str,
    overwrite_graph: bool,
    report: JobReport,
    standalone: bool,
    base_dir: str,
) -> tuple[list[ComputeResult], list[WriteResult]]:
    report.job_section(graph_name)
    graph = _project_one(gds, job.project, graph_name, overwrite=overwrite_graph, report=report, base_dir=base_dir)

    mutated = job.mutated_properties
    if standalone:
        # No DB write-back: results are streamed from the graph, so every written
        # property must be materialized (mutated) first.
        mutated = mutated | {write.node_property for write in job.write}
    writes_by_property = {w.node_property: w for w in job.write}
    compute_results: list[ComputeResult] = []
    write_results: list[WriteResult] = []
    for spec in job.compute:
        handle, result = _run_one_compute(
            gds, graph, spec, graph_name, mutate=spec.result_property in mutated, report=report
        )
        compute_results.append(result)
        if not standalone:
            # Write this compute's property back straight away (rather than in a later
            # batch), from the in-session graph if it was mutated, else from the result
            # store via the compute handle.
            write = writes_by_property.get(spec.result_property)
            if write is not None:
                if spec.result_property in mutated:
                    write_results.append(_write_back_mutated(gds, graph, write, graph_name, report))
                else:
                    write_results.append(_write_direct(handle, write, graph_name, report))

    if standalone:
        # Group writes by their output file so properties sharing a file are streamed
        # together (one CSV/JSON with all their columns), in first-appearance order.
        groups: dict[str, list[WriteSpec]] = {}
        for write in job.write:
            assert write.output_file is not None  # validated: standalone write => outputFile set
            groups.setdefault(write.output_file, []).append(write)
        for output_file, group_writes in groups.items():
            write_results.extend(_stream_group(gds, graph, group_writes, output_file, graph_name, base_dir, report))

    _drop_one(gds, graph_name, report)
    return compute_results, write_results


def run_all(
    gds: AuraGraphDataScience,
    cfg: JobsConfig,
    overwrite_graph: bool = False,
    report: Optional[JobReport] = None,
    base_dir: str = ".",
) -> JobResult:
    """Run every job as an isolated unit: project, compute (materializing mutates in order),
    write, then drop - before moving on to the next job.

    ``overwrite_graph=True`` drops any existing same-named graph before projecting it.
    ``base_dir`` is the job config's directory, which a ``construct`` projection's
    ``file`` and (for a standalone session) each write's ``outputFile`` are resolved
    relative to. For a standalone session (no database) written properties are
    streamed to those files instead of written back to the DB.
    """
    report = report or JobReport()
    standalone = cfg.session.is_standalone
    all_computes: list[ComputeResult] = []
    all_writes: list[WriteResult] = []

    for index, job in enumerate(cfg.jobs):
        computes, writes = _run_one_job(
            gds, job, job_graph_name(index), overwrite_graph, report, standalone=standalone, base_dir=base_dir
        )
        all_computes.extend(computes)
        all_writes.extend(writes)

    report.summary()
    return {"computes": all_computes, "writes": all_writes}
