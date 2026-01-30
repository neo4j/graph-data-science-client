from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.hdbscan_endpoints import (
    HdbscanEndpoints,
    HdbscanMutateResult,
    HdbscanStatsResult,
    HdbscanWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class HdbscanCypherEndpoints(HdbscanEndpoints):
    """
    Implementation of the HDBSCAN algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        node_property: str,
        mutate_property: str,
        *,
        leaf_size: int = 1,
        samples: int = 10,
        min_cluster_size: int = 5,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        job_id: str | None = None,
        username: str | None = None,
    ) -> HdbscanMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            node_property=node_property,
            min_cluster_size=min_cluster_size,
            leaf_size=leaf_size,
            samples=samples,
            relationship_types=relationship_types,
            node_labels=node_labels,
            mutate_property=mutate_property,
            concurrency=concurrency,
            log_progress=log_progress,
            sudo=sudo,
            job_id=job_id,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.hdbscan.mutate", params=params, logging=log_progress
        ).iloc[0]

        return HdbscanMutateResult(**cypher_result.to_dict())

    def stats(
        self,
        G: GraphV2,
        node_property: str,
        *,
        leaf_size: int = 1,
        samples: int = 10,
        min_cluster_size: int = 5,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        job_id: str | None = None,
        username: str | None = None,
    ) -> HdbscanStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            node_property=node_property,
            min_cluster_size=min_cluster_size,
            samples=samples,
            leaf_size=leaf_size,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            log_progress=log_progress,
            sudo=sudo,
            job_id=job_id,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.hdbscan.stats", params=params, logging=log_progress
        ).iloc[0]

        return HdbscanStatsResult(**cypher_result.to_dict())

    def stream(
        self,
        G: GraphV2,
        node_property: str,
        *,
        leaf_size: int = 1,
        samples: int = 10,
        min_cluster_size: int = 5,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        job_id: str | None = None,
        username: str | None = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            min_cluster_size=min_cluster_size,
            node_property=node_property,
            samples=samples,
            leaf_size=leaf_size,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            log_progress=log_progress,
            sudo=sudo,
            job_id=job_id,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.hdbscan.stream", params=params, logging=log_progress
        )

        return cypher_result

    def write(
        self,
        G: GraphV2,
        node_property: str,
        write_property: str,
        *,
        leaf_size: int = 1,
        samples: int = 10,
        min_cluster_size: int = 5,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        write_concurrency: int | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        job_id: str | None = None,
        username: str | None = None,
    ) -> HdbscanWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            node_property=node_property,
            leaf_size=leaf_size,
            samples=samples,
            min_cluster_size=min_cluster_size,
            relationship_types=relationship_types,
            node_labels=node_labels,
            write_property=write_property,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
            log_progress=log_progress,
            sudo=sudo,
            job_id=job_id,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.hdbscan.write", params=params, logging=log_progress
        ).iloc[0]

        return HdbscanWriteResult(**cypher_result.to_dict())

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        node_property: str,
        *,
        leaf_size: int = 1,
        samples: int = 10,
        min_cluster_size: int = 5,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        job_id: str | None = None,
        username: str | None = None,
    ) -> EstimationResult:
        config = ConfigConverter.convert_to_gds_config(
            min_cluster_size=min_cluster_size,
            node_property=node_property,
            samples=samples,
            leaf_size=leaf_size,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            log_progress=log_progress,
            sudo=sudo,
            job_id=job_id,
            username=username,
        )

        return estimate_algorithm(
            G=G,
            query_runner=self._query_runner,
            algo_config=config,
            endpoint="gds.hdbscan.stats.estimate",
        )
