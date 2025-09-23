from typing import Any, List, Optional, Union

from pandas import DataFrame

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.node_embedding.fastrp_endpoints import (
    FastRPEndpoints,
    FastRPMutateResult,
    FastRPStatsResult,
    FastRPWriteResult,
)

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from ..api.estimation_result import EstimationResult
from .node_property_endpoints import NodePropertyEndpoints


class FastRPArrowEndpoints(FastRPEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: Optional[RemoteWriteBackClient] = None,
        show_progress: bool = True,
    ):
        self._node_property_endpoints = NodePropertyEndpoints(
            arrow_client, write_back_client, show_progress=show_progress
        )

    def mutate(
        self,
        G: GraphV2,
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

        result = self._node_property_endpoints.run_job_and_mutate("v2/embeddings.fastrp", G, config, mutate_property)

        return FastRPMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
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

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/embeddings.fastrp", G, config)

        return FastRPStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
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
            "v2/embeddings.fastrp", G, config, write_concurrency, concurrency, write_property
        )

        return FastRPWriteResult(**result)

    def estimate(
        self,
        G: Union[GraphV2, dict[str, Any]],
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
