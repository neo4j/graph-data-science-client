import pandas as pd

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.local_clustering_coefficient_endpoints import (
    LocalClusteringCoefficientEndpoints,
    LocalClusteringCoefficientMutateResult,
    LocalClusteringCoefficientStatsResult,
    LocalClusteringCoefficientWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class LocalClusteringCoefficientCypherEndpoints(LocalClusteringCoefficientEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        *,
        mutate_property: str,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
    ) -> LocalClusteringCoefficientMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            triangle_count_property=triangle_count_property,
            username=username,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.localClusteringCoefficient.mutate", params=params, logging=log_progress
        ).squeeze()

        return LocalClusteringCoefficientMutateResult(**cypher_result.to_dict())

    def stats(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
    ) -> LocalClusteringCoefficientStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            triangle_count_property=triangle_count_property,
            username=username,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.localClusteringCoefficient.stats", params=params, logging=log_progress
        ).squeeze()

        return LocalClusteringCoefficientStatsResult(**cypher_result.to_dict())

    def stream(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
    ) -> pd.DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            triangle_count_property=triangle_count_property,
            username=username,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(
            endpoint="gds.localClusteringCoefficient.stream", params=params, logging=log_progress
        )

    def write(
        self,
        G: GraphV2,
        *,
        write_property: str,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> LocalClusteringCoefficientWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            write_property=write_property,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            triangle_count_property=triangle_count_property,
            username=username,
            write_concurrency=write_concurrency,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.localClusteringCoefficient.write", params=params, logging=log_progress
        ).squeeze()

        return LocalClusteringCoefficientWriteResult(**cypher_result.to_dict())

    def estimate(
        self,
        G: GraphV2,
        *,
        concurrency: int | None = None,
        job_id: str | None = None,
        log_progress: bool = True,
        node_labels: list[str] | None = None,
        relationship_types: list[str] | None = None,
        sudo: bool | None = False,
        triangle_count_property: str | None = None,
        username: str | None = None,
    ) -> EstimationResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            triangle_count_property=triangle_count_property,
            username=username,
        )

        return estimate_algorithm(
            "gds.localClusteringCoefficient.stream.estimate",
            self._query_runner,
            G,
            config,
        )
