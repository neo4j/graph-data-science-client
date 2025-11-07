from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.node_embedding.fastrp_endpoints import (
    FastRPEndpoints,
    FastRPMutateResult,
    FastRPStatsResult,
    FastRPWriteResult,
)
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper


class FastRPArrowEndpoints(FastRPEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client, write_back_client, show_progress=show_progress
        )

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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> FastRPMutateResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            embedding_dimension=embedding_dimension,
            feature_properties=feature_properties,
            iteration_weights=iteration_weights,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            node_self_influence=node_self_influence,
            normalization_strength=normalization_strength,
            property_ratio=property_ratio,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/embeddings.fastrp", config, mutate_property)

        return FastRPMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        embedding_dimension: int,
        iteration_weights: list[float] | None = None,
        normalization_strength: float | None = None,
        node_self_influence: float | None = None,
        property_ratio: float | None = None,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> FastRPStatsResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            embedding_dimension=embedding_dimension,
            feature_properties=feature_properties,
            iteration_weights=iteration_weights,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            node_self_influence=node_self_influence,
            normalization_strength=normalization_strength,
            property_ratio=property_ratio,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/embeddings.fastrp", config)

        return FastRPStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        embedding_dimension: int,
        iteration_weights: list[float] | None = None,
        normalization_strength: float | None = None,
        node_self_influence: float | None = None,
        property_ratio: float | None = None,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> DataFrame:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            embedding_dimension=embedding_dimension,
            feature_properties=feature_properties,
            iteration_weights=iteration_weights,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            node_self_influence=node_self_influence,
            normalization_strength=normalization_strength,
            property_ratio=property_ratio,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/embeddings.fastrp", G, config)

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
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool | None = None,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: Any | None = None,
        job_id: Any | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
        write_concurrency: int | None = None,
    ) -> FastRPWriteResult:
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            embedding_dimension=embedding_dimension,
            feature_properties=feature_properties,
            iteration_weights=iteration_weights,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            node_self_influence=node_self_influence,
            normalization_strength=normalization_strength,
            property_ratio=property_ratio,
            random_seed=random_seed,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sudo=sudo,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/embeddings.fastrp",
            G,
            config,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )

        return FastRPWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        embedding_dimension: int,
        iteration_weights: list[float] | None = None,
        normalization_strength: float | None = None,
        node_self_influence: float | None = None,
        property_ratio: float | None = None,
        feature_properties: list[str] | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: Any | None = None,
        relationship_weight_property: str | None = None,
        random_seed: Any | None = None,
    ) -> EstimationResult:
        config = self._node_property_endpoints.create_estimate_config(
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
        return self._node_property_endpoints.estimate("v2/embeddings.fastrp.estimate", G, config)
