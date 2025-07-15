from typing import Any, List, Optional, Union

from pandas import DataFrame

from ...call_parameters import CallParameters
from ...graph.graph_object import Graph
from ...query_runner.query_runner import QueryRunner
from ..api.estimation_result import EstimationResult
from ..api.wcc_endpoints import WccEndpoints, WccMutateResult, WccStatsResult, WccWriteResult
from ..utils.config_converter import ConfigConverter


class WccCypherEndpoints(WccEndpoints):
    """
    Implementation of the WCC algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        threshold: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> WccMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            threshold=threshold,
            username=username,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(endpoint="gds.wcc.mutate", params=params).squeeze()

        return WccMutateResult(**cypher_result.to_dict())

    def stats(
        self,
        G: Graph,
        threshold: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> WccStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            threshold=threshold,
            username=username,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(endpoint="gds.wcc.stats", params=params).squeeze()  # type: ignore

        return WccStatsResult(**cypher_result.to_dict())

    def stream(
        self,
        G: Graph,
        min_component_size: Optional[int] = None,
        threshold: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            min_component_size=min_component_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            threshold=threshold,
            username=username,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.wcc.stream", params=params)

    def write(
        self,
        G: Graph,
        write_property: str,
        min_component_size: Optional[int] = None,
        threshold: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[int] = None,
    ) -> WccWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            write_property=write_property,
            concurrency=concurrency,
            consecutive_ids=consecutive_ids,
            job_id=job_id,
            log_progress=log_progress,
            min_component_size=min_component_size,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            seed_property=seed_property,
            sudo=sudo,
            threshold=threshold,
            username=username,
        )

        if write_concurrency is not None:
            config["writeConcurrency"] = write_concurrency

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.wcc.write", params=params).squeeze()  # type: ignore

        return WccWriteResult(**result.to_dict())

    def estimate(
        self, graph_name: Optional[str] = None, projection_config: Optional[dict[str, Any]] = None
    ) -> EstimationResult:
        config: Union[str, dict[str, Any]] = {}

        if graph_name is not None:
            config = graph_name
        elif projection_config is not None:
            config = projection_config
        else:
            raise ValueError("Either graph_name or projection_config must be provided.")

        params = CallParameters(config=config)

        result = self._query_runner.call_procedure(endpoint="gds.wcc.stats.estimate", params=params).squeeze()

        return EstimationResult(**result.to_dict())
