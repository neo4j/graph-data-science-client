from typing import Any, List

from pydantic.alias_generators import to_snake

from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from .job_config import (
    AlgorithmStep,
    CypherProjection,
    JobConfig,
    NativeProjection,
    Projection,
    Step,
    WriteBackStep, GraphParams, )

# GDS algorithm names that don't resolve to their AuraGraphDataScience client attribute via a
# mechanical camelCase -> snake_case conversion (pydantic.alias_generators.to_snake).
_ALGORITHM_ENDPOINT_ALIASES: dict[str, str] = {
    "node2vec": "node2vec",
    "k1coloring": "k1_coloring",
    "kcore": "k_core_decomposition",
    "celf": "influence_maximization_celf",
    "degree": "degree_centrality",
    "closeness": "closeness_centrality",
    "harmonic": "harmonic_centrality",
    "eigenvector": "eigenvector_centrality",
    "betweenness": "betweenness_centrality",
}


class JobExecutionError(Exception):
    pass


class JobPipeline:
    def __init__(self, projection: Projection) -> None:
        self._graph = projection.graph
        self._projection = projection
        self._steps = []

    def is_empty(self) -> bool:
        return len(self._steps) == 0

    def add_step(self, step: Step) -> None:
        if not isinstance(step.params, GraphParams) or step.params.graph != self._graph:
            raise ValueError(f"Step graph '{step.params['graph']}' does not match projection graph '{self._graph}'.")
        self._steps.append(step)

    def execute(self, executor: "JobExecutor") -> None:
        executor.run_projection(self._projection)
        try:
            for step in self._steps:
                executor.run_step(step)
        finally:
            executor.run_graph_drop(self._graph)


class JobPipelinesBuilder:

    def __init__(self) -> None:
        self._pipelines: dict[str, JobPipeline] = {}

    def add_projection(self, projection: Projection) -> None:
        if self._pipelines.get(projection.graph) is not None:
            raise JobExecutionError(f"A pipeline for graph {projection.graph} already exists.")

        self._pipelines[projection.graph] = JobPipeline(projection)

    def add_step(self, step: Step) -> None:
        if not isinstance(step.params, GraphParams) or step.params.graph != self._graph:
            raise ValueError(f"Step graph '{step.params['graph']}' does not match projection graph '{self._graph}'.")
        self._pipelines[step.params.graph].add_step(step)

    def build(self) -> List[JobPipeline]:
        """
        Constructs a list of pipelines, each containing a projection and the steps that reference it.
        Projections that are not referenced by any step are skipped and will not be executed.
        """
        return [p for p in self._pipelines.values() if not p.is_empty()]


class JobExecutor:
    """
    Executes a `JobConfig` by dynamically resolving and calling the corresponding
    projection, algorithm and write-back endpoints on an `AuraGraphDataScience` (GDS session) client instance.

    Graphs are projected lazily: a graph declared in `projections` is only projected right before the
    first step that references it, and it is dropped again right after the last step that references it
    has finished. Declared projections that no step references are never projected.
    """

    def __init__(self, gds: AuraGraphDataScience) -> None:
        self._gds = gds

    def run(self, job_config: JobConfig) -> None:
        pipeline_builder = JobPipelinesBuilder()
        for projection in job_config.projections:
            pipeline_builder.add_projection(projection)

        for step in job_config.steps:
            pipeline_builder.add_step(step)

        job_pipelines = pipeline_builder.build()

        for job_pipeline in job_pipelines:
            job_pipeline.execute(self)

    def run_step(self, step: Step) -> Any:
        if isinstance(step, AlgorithmStep):
            return self._run_algorithm(step)
        if isinstance(step, WriteBackStep):
            return self._run_write_back(step)
        raise JobExecutionError(f"Unsupported step type '{step.type}'.")

    def run_projection(self, projection: Projection) -> None:
        spec = projection.spec
        if isinstance(spec, CypherProjection):
            self._gds.graph.project.cypher(spec.graph, spec.query)
            return

        assert isinstance(spec, NativeProjection)
        self._gds.graph.project.native(
            spec.graph,
            node_label_filter=spec.node_labels or ALL_LABELS,
            relationship_type_filter=spec.relationship_types or ALL_TYPES,
            node_properties=spec.node_properties,
            relationship_properties=spec.relationship_properties,
            undirected_relationship_types=spec.undirected_relationship_types,
            inverse_indexed_relationship_types=spec.inverse_indexed_relationship_types,
        )

    def run_graph_drop(self, graph: str) -> None:
        self._gds.graph.drop(graph)


    def _run_algorithm(self, step: AlgorithmStep) -> Any:
        params = step.params
        endpoints = self._resolve_algorithm_endpoints(params.algorithm)
        graph = self._gds.graph.get(params.graph)
        config = {to_snake(key): value for key, value in params.configuration.items()}

        if params.mode == "stream":
            return endpoints.stream(graph, **config)

        method = getattr(endpoints, params.mode.name)
        property_kwarg = f"{params.mode.name}_property"
        return method(graph, **{property_kwarg: params.mode.property}, **config)

    def _run_write_back(self, step: WriteBackStep) -> list[Any]:
        params = step.params
        graph = self._gds.graph.get(params.graph)

        results: list[Any] = []
        if params.node_properties:
            results.append(self._gds.graph.node_properties.write(graph, params.node_properties))
        for relationship_type in params.relationship_types or []:
            results.append(self._gds.graph.relationships.write(graph, relationship_type, params.relationship_properties))
        return results

    def _resolve_algorithm_endpoints(self, name: str) -> Any:
        attr_name = _ALGORITHM_ENDPOINT_ALIASES.get(name, to_snake(name))
        endpoints = getattr(self._gds, attr_name, None)
        if endpoints is None:
            raise JobExecutionError(
                f"Could not resolve algorithm '{name}' to an endpoint on the AuraGraphDataScience client "
                f"(tried attribute '{attr_name}')."
            )
        return endpoints
