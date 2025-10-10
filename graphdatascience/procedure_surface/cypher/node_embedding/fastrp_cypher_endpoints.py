from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.node_embedding.fastrp_endpoints import (
    FastRPEndpoints,
    FastRPMutateResult,
    FastRPStatsResult,
    FastRPWriteResult,
)
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class FastRPCypherEndpoints(FastRPEndpoints):
    """
    Implementation of the FastRP algorithm endpoints.
    This class handles the actual execution by forwarding calls to the query runner.
    """

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        embedding_dimension: int,
        iteration_weights: list[float] | None = None,
        normalization_strength: float | None = None,
        node_self_influence: float | None = None,
        property_ratio: float | None = None,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
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

        result = self._query_runner.call_procedure(
            endpoint="gds.fastRP.mutate", params=params, logging=log_progress
        ).squeeze()

        return FastRPMutateResult(**result.to_dict())

    def stats(
        self,
        G: GraphV2,
        embedding_dimension: int,
        iteration_weights: list[float] | None = None,
        normalization_strength: float | None = None,
        node_self_influence: float | None = None,
        property_ratio: float | None = None,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
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

        result = self._query_runner.call_procedure(
            endpoint="gds.fastRP.stats", params=params, logging=log_progress
        ).squeeze()

        return FastRPStatsResult(**result.to_dict())

    def stream(
        self,
        G: GraphV2,
        embedding_dimension: int,
        iteration_weights: list[float] | None = None,
        normalization_strength: float | None = None,
        node_self_influence: float | None = None,
        property_ratio: float | None = None,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
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

        result = self._query_runner.call_procedure(endpoint="gds.fastRP.stream", params=params, logging=log_progress)

        return result

    def write(
        self,
        G: GraphV2,
        write_property: str,
        embedding_dimension: int,
        iteration_weights: list[float] | None = None,
        normalization_strength: float | None = None,
        node_self_influence: float | None = None,
        property_ratio: float | None = None,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
        write_concurrency: int | None = None,
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

        result = self._query_runner.call_procedure(
            endpoint="gds.fastRP.write", params=params, logging=log_progress
        ).squeeze()

        return FastRPWriteResult(**result.to_dict())

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        embedding_dimension: int,
        iteration_weights: list[float] | None = None,
        normalization_strength: float | None = None,
        node_self_influence: float | None = None,
        property_ratio: float | None = None,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] | None = None,
        node_labels: list[str] | None = None,
        concurrency: Any | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
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
