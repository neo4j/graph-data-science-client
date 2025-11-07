from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.centrality.closeness_harmonic_endpoints import (
    ClosenessHarmonicEndpoints,
    ClosenessHarmonicMutateResult,
    ClosenessHarmonicStatsResult,
    ClosenessHarmonicWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class ClosenessHarmonicCypherEndpoints(ClosenessHarmonicEndpoints):
    """Cypher-based implementation of Harmonic Closeness Centrality algorithm endpoints."""

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> ClosenessHarmonicMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutateProperty=mutate_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.closeness.harmonic.mutate", params=params, logging=log_progress
        ).squeeze()
        return ClosenessHarmonicMutateResult(**result.to_dict())

    def stats(
        self,
        G: GraphV2,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> ClosenessHarmonicStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.closeness.harmonic.stats", params=params, logging=log_progress
        ).squeeze()
        return ClosenessHarmonicStatsResult(**result.to_dict())

    def stream(
        self,
        G: GraphV2,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(
            endpoint="gds.closeness.harmonic.stream", params=params, logging=log_progress
        )

    def write(
        self,
        G: GraphV2,
        write_property: str,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        write_concurrency: int | None = None,
    ) -> ClosenessHarmonicWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            writeProperty=write_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
            writeConcurrency=write_concurrency,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            endpoint="gds.closeness.harmonic.write", params=params, logging=log_progress
        ).squeeze()
        return ClosenessHarmonicWriteResult(**result.to_dict())

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: Any | None = None,
    ) -> EstimationResult:
        algo_config = ConfigConverter.convert_to_gds_config(
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            concurrency=concurrency,
        )

        return estimate_algorithm(
            endpoint="gds.closeness.harmonic.stats.estimate",
            query_runner=self._query_runner,
            G=G,
            algo_config=algo_config,
        )
