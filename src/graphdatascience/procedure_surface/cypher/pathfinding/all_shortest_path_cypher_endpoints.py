from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.pathfinding.all_shortest_path_endpoints import AllShortestPathEndpoints
from graphdatascience.procedure_surface.api.pathfinding.single_source_delta_endpoints import SingleSourceDeltaEndpoints
from graphdatascience.procedure_surface.api.pathfinding.single_source_dijkstra_endpoints import (
    SingleSourceDijkstraEndpoints,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.cypher.pathfinding.single_source_delta_cypher_endpoints import (
    DeltaSteppingCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.pathfinding.single_source_dijkstra_cypher_endpoints import (
    SingleSourceDijkstraCypherEndpoints,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class AllShortestPathCypherEndpoints(AllShortestPathEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner
        self._delta = DeltaSteppingCypherEndpoints(query_runner)
        self._dijkstra = SingleSourceDijkstraCypherEndpoints(query_runner)

    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] = ALL_LABELS,
        relationship_types: list[str] = ALL_TYPES,
        relationship_weight_property: str | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(
            endpoint="gds.allShortestPaths.stream", params=params, logging=log_progress
        )

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        *,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        config = ConfigConverter.convert_to_gds_config(
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            username=username,
            concurrency=concurrency,
        )

        return estimate_algorithm("gds.allShortestPaths.stream.estimate", self._query_runner, G, config)

    @property
    def delta(self) -> SingleSourceDeltaEndpoints:
        return self._delta

    @property
    def dijkstra(self) -> SingleSourceDijkstraEndpoints:
        return self._dijkstra
