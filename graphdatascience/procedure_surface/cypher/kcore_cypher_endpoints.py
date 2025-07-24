from collections import OrderedDict
from typing import Any, List, Optional, Union

from pandas import DataFrame

from ...call_parameters import CallParameters
from ...graph.graph_object import Graph
from ...query_runner.query_runner import QueryRunner
from ..api.estimation_result import EstimationResult
from ..api.kcore_endpoints import KCoreEndpoints, KCoreMutateResult, KCoreStatsResult, KCoreWriteResult
from ..utils.config_converter import ConfigConverter


class KCoreCypherEndpoints(KCoreEndpoints):
    """
    Implementation of the K-Core algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        target_nodes: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> KCoreMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
            target_nodes=target_nodes,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(endpoint="gds.kcore.mutate", params=params).squeeze()

        return KCoreMutateResult(**cypher_result.to_dict())

    def stats(
        self,
        G: Graph,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        target_nodes: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> KCoreStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
            target_nodes=target_nodes,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(endpoint="gds.kcore.stats", params=params).squeeze()

        return KCoreStatsResult(**cypher_result.to_dict())

    def stream(
        self,
        G: Graph,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        target_nodes: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
            target_nodes=target_nodes,
            username=username,
        )

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.kcore.stream", params=params)

    def write(
        self,
        G: Graph,
        write_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[int] = None,
        job_id: Optional[str] = None,
        target_nodes: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[int] = None,
    ) -> KCoreWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            write_property=write_property,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
            target_nodes=target_nodes,
            username=username,
        )

        if write_concurrency is not None:
            config["writeConcurrency"] = write_concurrency

        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.kcore.write", params=params).squeeze()

        return KCoreWriteResult(**result.to_dict())

    def estimate(
        self, G: Optional[Graph] = None, projection_config: Optional[dict[str, Any]] = None
    ) -> EstimationResult:
        config: Union[dict[str, Any]] = OrderedDict()

        if G is not None:
            config["graphNameOrConfiguration"] = G.name()
        elif projection_config is not None:
            config["graphNameOrConfiguration"] = projection_config
        else:
            raise ValueError("Either graph_name or projection_config must be provided.")

        config["algoConfig"] = {}

        params = CallParameters(**config)

        result = self._query_runner.call_procedure(endpoint="gds.kcore.stats.estimate", params=params).squeeze()

        return EstimationResult(**result.to_dict())
