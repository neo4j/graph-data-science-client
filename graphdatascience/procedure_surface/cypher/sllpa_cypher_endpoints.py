from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.sllpa_endpoints import (
    SllpaEndpoints,
    SllpaMutateResult,
    SllpaStatsResult,
    SllpaWriteResult,
)
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class SllpaCypherEndpoints(SllpaEndpoints):
    """
    Implementation of the SLLPA algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        *,
        max_iterations: int,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        min_association_strength: Optional[float] = None,
        node_labels: Optional[List[str]] = None,
        partitioning: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> SllpaMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            max_iterations=max_iterations,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            min_association_strength=min_association_strength,
            node_labels=node_labels,
            partitioning=partitioning,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(
            graph_name=G.name(),
            configuration=config,
        )

        result = self._query_runner.call_procedure(endpoint="gds.sllpa.mutate", params=params).squeeze()

        return SllpaMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        *,
        max_iterations: int,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        min_association_strength: Optional[float] = None,
        node_labels: Optional[List[str]] = None,
        partitioning: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> SllpaStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            max_iterations=max_iterations,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            min_association_strength=min_association_strength,
            node_labels=node_labels,
            partitioning=partitioning,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(
            graph_name=G.name(),
            configuration=config,
        )

        result = self._query_runner.call_procedure(endpoint="gds.sllpa.stats", params=params).squeeze()

        return SllpaStatsResult(**result)

    def stream(
        self,
        G: GraphV2,
        *,
        max_iterations: int,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        min_association_strength: Optional[float] = None,
        node_labels: Optional[List[str]] = None,
        partitioning: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            max_iterations=max_iterations,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            min_association_strength=min_association_strength,
            node_labels=node_labels,
            partitioning=partitioning,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        params = CallParameters(
            graph_name=G.name(),
            configuration=config,
        )

        result = self._query_runner.call_procedure(endpoint="gds.sllpa.stream", params=params)

        return result

    def write(
        self,
        G: GraphV2,
        write_property: str,
        *,
        max_iterations: int,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        log_progress: bool = True,
        min_association_strength: Optional[float] = None,
        node_labels: Optional[List[str]] = None,
        partitioning: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
        sudo: Optional[bool] = False,
        username: Optional[str] = None,
        write_concurrency: Optional[int] = None,
    ) -> SllpaWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            write_property=write_property,
            max_iterations=max_iterations,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            min_association_strength=min_association_strength,
            node_labels=node_labels,
            partitioning=partitioning,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
            write_concurrency=write_concurrency,
        )

        params = CallParameters(
            graph_name=G.name(),
            configuration=config,
        )

        result = self._query_runner.call_procedure(endpoint="gds.sllpa.write", params=params).squeeze()

        return SllpaWriteResult(**result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
        *,
        max_iterations: int,
        concurrency: Optional[int] = None,
        min_association_strength: Optional[float] = None,
        node_labels: Optional[List[str]] = None,
        partitioning: Optional[Any] = None,
        relationship_types: Optional[List[str]] = None,
    ) -> EstimationResult:
        from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm

        config = ConfigConverter.convert_to_gds_config(
            max_iterations=max_iterations,
            concurrency=concurrency,
            min_association_strength=min_association_strength,
            node_labels=node_labels,
            partitioning=partitioning,
            relationship_types=relationship_types,
        )

        return estimate_algorithm("gds.sllpa.stream.estimate", self._query_runner, G, config)
