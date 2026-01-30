from __future__ import annotations

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.pathfinding.k_spanning_tree_endpoints import (
    KSpanningTreeEndpoints,
    KSpanningTreeWriteResult,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class KSpanningTreeCypherEndpoints(KSpanningTreeEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def write(
        self,
        G: GraphV2,
        k: int,
        write_property: str,
        source_node: int,
        relationship_weight_property: str | None = None,
        objective: str = "minimum",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> KSpanningTreeWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            k=k,
            writeProperty=write_property,
            sourceNode=source_node,
            relationshipWeightProperty=relationship_weight_property,
            objective=objective,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
            writeConcurrency=write_concurrency,
        )
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            "gds.kSpanningTree.write", params=params, logging=log_progress
        ).squeeze()

        return KSpanningTreeWriteResult(**result)
