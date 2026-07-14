from __future__ import annotations

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.centrality.hits_endpoints import (
    HitsEndpoints,
    HitsMutateResult,
    HitsStatsResult,
    HitsWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class HitsCypherEndpoints(HitsEndpoints):
    """
    Implementation of the HITS algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: Graph,
        *,
        mutate_property: str = "",
        hits_iterations: int = 20,
        auth_property: str = "auth",
        hub_property: str = "hub",
        partitioning: str = "AUTO",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> HitsMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            hits_iterations=hits_iterations,
            auth_property=auth_property,
            hub_property=hub_property,
            partitioning=partitioning,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.hits.mutate", params=params, logging=log_progress
        ).iloc[0]

        return HitsMutateResult(**cypher_result)

    def stats(
        self,
        G: Graph,
        *,
        hits_iterations: int = 20,
        partitioning: str = "AUTO",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> HitsStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            hits_iterations=hits_iterations,
            partitioning=partitioning,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.hits.stats", params=params, logging=log_progress
        ).iloc[0]

        return HitsStatsResult(**cypher_result)

    def stream(
        self,
        G: Graph,
        *,
        hits_iterations: int = 20,
        auth_property: str = "auth",
        hub_property: str = "hub",
        partitioning: str = "AUTO",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            hits_iterations=hits_iterations,
            auth_property=auth_property,
            hub_property=hub_property,
            partitioning=partitioning,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.hits.stream", params=params, logging=log_progress)

    def write(
        self,
        G: Graph,
        *,
        write_property: str = "",
        hits_iterations: int = 20,
        auth_property: str = "auth",
        hub_property: str = "hub",
        partitioning: str = "AUTO",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> HitsWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            write_property=write_property,
            hits_iterations=hits_iterations,
            auth_property=auth_property,
            hub_property=hub_property,
            partitioning=partitioning,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
            write_concurrency=write_concurrency,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.hits.write", params=params, logging=log_progress
        ).iloc[0]

        return HitsWriteResult(**cypher_result)
