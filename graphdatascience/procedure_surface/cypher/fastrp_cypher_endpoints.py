from typing import Any, List, Optional, Union

from pandas import DataFrame

from ...call_parameters import CallParameters
from ...graph.graph_object import Graph
from ...query_runner.query_runner import QueryRunner
from ..api.estimation_result import EstimationResult
from ..api.fastrp_endpoints import (
    FastRPEndpoints,
    FastRPMutateResult,
    FastRPStatsResult,
    FastRPWriteResult,
)
from ..utils.config_converter import ConfigConverter
from .estimation_utils import estimate_algorithm


class FastRPCypherEndpoints(FastRPEndpoints):
    """
    Implementation of the FastRP algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        embedding_dimension: int,
        iteration_weights: Optional[List[float]] = None,
        normalization_strength: Optional[float] = None,
        node_self_influence: Optional[float] = None,
        property_ratio: Optional[float] = None,
        feature_properties: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
    ) -> FastRPMutateResult:
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            embedding_dimension=embedding_dimension,
            iteration_weights=iteration_weights,
            normalization_strength=normalization_strength,
            node_self_influence=node_self_influence,
            property_ratio=property_ratio,
            feature_properties=feature_properties,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
            relationship_weight_property=relationship_weight_property,
            random_seed=random_seed,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.fastRP.mutate", params=params).squeeze()

        return FastRPMutateResult(**result.to_dict())

    def stats(
        self,
        G: Graph,
        embedding_dimension: int,
        iteration_weights: Optional[List[float]] = None,
        normalization_strength: Optional[float] = None,
        node_self_influence: Optional[float] = None,
        property_ratio: Optional[float] = None,
        feature_properties: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
    ) -> FastRPStatsResult:
        config = ConfigConverter.convert_to_gds_config(
            embedding_dimension=embedding_dimension,
            iteration_weights=iteration_weights,
            normalization_strength=normalization_strength,
            node_self_influence=node_self_influence,
            property_ratio=property_ratio,
            feature_properties=feature_properties,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
            relationship_weight_property=relationship_weight_property,
            random_seed=random_seed,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.fastRP.stats", params=params).squeeze()

        return FastRPStatsResult(**result.to_dict())

    def stream(
        self,
        G: Graph,
        embedding_dimension: int,
        iteration_weights: Optional[List[float]] = None,
        normalization_strength: Optional[float] = None,
        node_self_influence: Optional[float] = None,
        property_ratio: Optional[float] = None,
        feature_properties: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
    ) -> DataFrame:
        config = ConfigConverter.convert_to_gds_config(
            embedding_dimension=embedding_dimension,
            iteration_weights=iteration_weights,
            normalization_strength=normalization_strength,
            node_self_influence=node_self_influence,
            property_ratio=property_ratio,
            feature_properties=feature_properties,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
            relationship_weight_property=relationship_weight_property,
            random_seed=random_seed,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.fastRP.stream", params=params)

        return result

    def write(
        self,
        G: Graph,
        write_property: str,
        embedding_dimension: int,
        iteration_weights: Optional[List[float]] = None,
        normalization_strength: Optional[float] = None,
        node_self_influence: Optional[float] = None,
        property_ratio: Optional[float] = None,
        feature_properties: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
        write_concurrency: Optional[int] = None,
    ) -> FastRPWriteResult:
        config = ConfigConverter.convert_to_gds_config(
            write_property=write_property,
            embedding_dimension=embedding_dimension,
            iteration_weights=iteration_weights,
            normalization_strength=normalization_strength,
            node_self_influence=node_self_influence,
            property_ratio=property_ratio,
            feature_properties=feature_properties,
            relationship_types=relationship_types,
            node_labels=node_labels,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            concurrency=concurrency,
            job_id=job_id,
            relationship_weight_property=relationship_weight_property,
            random_seed=random_seed,
            write_concurrency=write_concurrency,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(endpoint="gds.fastRP.write", params=params).squeeze()

        return FastRPWriteResult(**result.to_dict())

    def estimate(
        self,
        G: Union[Graph, dict[str, Any]],
        embedding_dimension: int,
        iteration_weights: Optional[List[float]] = None,
        normalization_strength: Optional[float] = None,
        node_self_influence: Optional[float] = None,
        property_ratio: Optional[float] = None,
        feature_properties: Optional[List[str]] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        concurrency: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        random_seed: Optional[Any] = None,
    ) -> EstimationResult:
        algo_config = ConfigConverter.convert_to_gds_config(
            embedding_dimension=embedding_dimension,
            iteration_weights=iteration_weights,
            normalization_strength=normalization_strength,
            node_self_influence=node_self_influence,
            property_ratio=property_ratio,
            feature_properties=feature_properties,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            relationship_weight_property=relationship_weight_property,
            random_seed=random_seed,
        )
        return estimate_algorithm(
            endpoint="gds.fastRP.stats.estimate", query_runner=self._query_runner, G=G, algo_config=algo_config
        )
