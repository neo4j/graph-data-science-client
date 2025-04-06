from typing import Any, List, Optional

from pandas import DataFrame, Series

from ...call_parameters import CallParameters
from ...graph.graph_object import Graph
from ...query_runner.query_runner import QueryRunner
from ..api.wcc_endpoints import WccEndpoints


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
    ) -> Series[Any]:
        # Build configuration dictionary from parameters
        config: dict[str, Any] = {
            "mutateProperty": mutate_property,
        }

        # Add optional parameters
        if threshold is not None:
            config["threshold"] = threshold
        if relationship_types is not None:
            config["relationshipTypes"] = relationship_types
        if node_labels is not None:
            config["nodeLabels"] = node_labels
        if sudo is not None:
            config["sudo"] = sudo
        if log_progress is not None:
            config["logProgress"] = log_progress
        if username is not None:
            config["username"] = username
        if concurrency is not None:
            config["concurrency"] = concurrency
        if job_id is not None:
            config["jobId"] = job_id
        if seed_property is not None:
            config["seedProperty"] = seed_property
        if consecutive_ids is not None:
            config["consecutiveIds"] = consecutive_ids
        if relationship_weight_property is not None:
            config["relationshipWeightProperty"] = relationship_weight_property

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.wcc.mutate", params=params).squeeze()  # type: ignore

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
    ) -> Series[Any]:
        # Build configuration dictionary from parameters
        config: dict[str, Any] = {}

        # Add optional parameters
        if threshold is not None:
            config["threshold"] = threshold
        if relationship_types is not None:
            config["relationshipTypes"] = relationship_types
        if node_labels is not None:
            config["nodeLabels"] = node_labels
        if sudo is not None:
            config["sudo"] = sudo
        if log_progress is not None:
            config["logProgress"] = log_progress
        if username is not None:
            config["username"] = username
        if concurrency is not None:
            config["concurrency"] = concurrency
        if job_id is not None:
            config["jobId"] = job_id
        if seed_property is not None:
            config["seedProperty"] = seed_property
        if consecutive_ids is not None:
            config["consecutiveIds"] = consecutive_ids
        if relationship_weight_property is not None:
            config["relationshipWeightProperty"] = relationship_weight_property

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.wcc.stats", params=params).squeeze()  # type: ignore

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
        # Build configuration dictionary from parameters
        config: dict[str, Any] = {}

        # Add optional parameters
        if min_component_size is not None:
            config["minComponentSize"] = min_component_size
        if threshold is not None:
            config["threshold"] = threshold
        if relationship_types is not None:
            config["relationshipTypes"] = relationship_types
        if node_labels is not None:
            config["nodeLabels"] = node_labels
        if sudo is not None:
            config["sudo"] = sudo
        if log_progress is not None:
            config["logProgress"] = log_progress
        if username is not None:
            config["username"] = username
        if concurrency is not None:
            config["concurrency"] = concurrency
        if job_id is not None:
            config["jobId"] = job_id
        if seed_property is not None:
            config["seedProperty"] = seed_property
        if consecutive_ids is not None:
            config["consecutiveIds"] = consecutive_ids
        if relationship_weight_property is not None:
            config["relationshipWeightProperty"] = relationship_weight_property

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
        write_to_result_store: Optional[bool] = None,
    ) -> Series[Any]:
        # Build configuration dictionary from parameters
        config: dict[str, Any] = {
            "writeProperty": write_property,
        }

        # Add optional parameters
        if min_component_size is not None:
            config["minComponentSize"] = min_component_size
        if threshold is not None:
            config["threshold"] = threshold
        if relationship_types is not None:
            config["relationshipTypes"] = relationship_types
        if node_labels is not None:
            config["nodeLabels"] = node_labels
        if sudo is not None:
            config["sudo"] = sudo
        if log_progress is not None:
            config["logProgress"] = log_progress
        if username is not None:
            config["username"] = username
        if concurrency is not None:
            config["concurrency"] = concurrency
        if job_id is not None:
            config["jobId"] = job_id
        if seed_property is not None:
            config["seedProperty"] = seed_property
        if consecutive_ids is not None:
            config["consecutiveIds"] = consecutive_ids
        if relationship_weight_property is not None:
            config["relationshipWeightProperty"] = relationship_weight_property
        if write_concurrency is not None:
            config["writeConcurrency"] = write_concurrency
        if write_to_result_store is not None:
            config["writeToResultStore"] = write_to_result_store

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.wcc.write", params=params).squeeze()  # type: ignore
