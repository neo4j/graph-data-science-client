from __future__ import annotations

from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.similarity.node_similarity_filtered_endpoints import (
    NodeSimilarityFilteredEndpoints,
)
from graphdatascience.procedure_surface.api.similarity.node_similarity_results import (
    NodeSimilarityMutateResult,
    NodeSimilarityStatsResult,
    NodeSimilarityWriteResult,
)
from graphdatascience.procedure_surface.arrow.relationship_endpoints_helper import RelationshipEndpointsHelper
from graphdatascience.procedure_surface.arrow.stream_result_mapper import rename_similarity_stream_result


class NodeSimilarityFilteredArrowEndpoints(NodeSimilarityFilteredEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None = None,
        show_progress: bool = False,
    ):
        self._endpoints_helper = RelationshipEndpointsHelper(
            arrow_client, write_back_client=write_back_client, show_progress=show_progress
        )

    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        mutate_property: str,
        source_node_filter: str | list[int],
        target_node_filter: str | list[int],
        top_k: int = 10,
        bottom_k: int = 10,
        top_n: int = 0,
        bottom_n: int = 0,
        similarity_cutoff: float = 1.0e-42,
        degree_cutoff: int = 1,
        upper_degree_cutoff: int = 2147483647,
        similarity_metric: str = "JACCARD",
        use_components: bool | str = False,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> NodeSimilarityMutateResult:
        config = self._endpoints_helper.create_base_config(
            G,
            sourceNodeFilter=source_node_filter,
            targetNodeFilter=target_node_filter,
            topK=top_k,
            bottomK=bottom_k,
            topN=top_n,
            bottomN=bottom_n,
            similarityCutoff=similarity_cutoff,
            degreeCutoff=degree_cutoff,
            upperDegreeCutoff=upper_degree_cutoff,
            similarityMetric=similarity_metric,
            useComponents=use_components,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        result = self._endpoints_helper.run_job_and_mutate(
            "v2/similarity.nodeSimilarity.filtered", config, mutate_property, mutate_relationship_type
        )

        return NodeSimilarityMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        source_node_filter: str | list[int],
        target_node_filter: str | list[int],
        top_k: int = 10,
        bottom_k: int = 10,
        top_n: int = 0,
        bottom_n: int = 0,
        similarity_cutoff: float = 1.0e-42,
        degree_cutoff: int = 1,
        upper_degree_cutoff: int = 2147483647,
        similarity_metric: str = "JACCARD",
        use_components: bool | str = False,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> NodeSimilarityStatsResult:
        config = self._endpoints_helper.create_base_config(
            G,
            sourceNodeFilter=source_node_filter,
            targetNodeFilter=target_node_filter,
            topK=top_k,
            bottomK=bottom_k,
            topN=top_n,
            bottomN=bottom_n,
            similarityCutoff=similarity_cutoff,
            degreeCutoff=degree_cutoff,
            upperDegreeCutoff=upper_degree_cutoff,
            similarityMetric=similarity_metric,
            useComponents=use_components,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        result = self._endpoints_helper.run_job_and_get_summary(
            "v2/similarity.nodeSimilarity.filtered",
            config,
        )
        if "similarityPairs" not in result:
            result["similarityPairs"] = result.get("relationshipsWritten", 0)

        return NodeSimilarityStatsResult(**result)

    def stream(
        self,
        G: GraphV2,
        source_node_filter: str | list[int],
        target_node_filter: str | list[int],
        top_k: int = 10,
        bottom_k: int = 10,
        top_n: int = 0,
        bottom_n: int = 0,
        similarity_cutoff: float = 1.0e-42,
        degree_cutoff: int = 1,
        upper_degree_cutoff: int = 2147483647,
        similarity_metric: str = "JACCARD",
        use_components: bool | str = False,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        config = self._endpoints_helper.create_base_config(
            G,
            sourceNodeFilter=source_node_filter,
            targetNodeFilter=target_node_filter,
            topK=top_k,
            bottomK=bottom_k,
            topN=top_n,
            bottomN=bottom_n,
            similarityCutoff=similarity_cutoff,
            degreeCutoff=degree_cutoff,
            upperDegreeCutoff=upper_degree_cutoff,
            similarityMetric=similarity_metric,
            useComponents=use_components,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
        )

        result = self._endpoints_helper.run_job_and_stream("v2/similarity.nodeSimilarity.filtered", G, config)

        rename_similarity_stream_result(result)
        return result

    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        write_property: str,
        source_node_filter: str | list[int],
        target_node_filter: str | list[int],
        top_k: int = 10,
        bottom_k: int = 10,
        top_n: int = 0,
        bottom_n: int = 0,
        similarity_cutoff: float = 1.0e-42,
        degree_cutoff: int = 1,
        upper_degree_cutoff: int = 2147483647,
        similarity_metric: str = "JACCARD",
        use_components: bool | str = False,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> NodeSimilarityWriteResult:
        config = self._endpoints_helper.create_base_config(
            G,
            sourceNodeFilter=source_node_filter,
            targetNodeFilter=target_node_filter,
            topK=top_k,
            bottomK=bottom_k,
            topN=top_n,
            bottomN=bottom_n,
            similarityCutoff=similarity_cutoff,
            degreeCutoff=degree_cutoff,
            upperDegreeCutoff=upper_degree_cutoff,
            similarityMetric=similarity_metric,
            useComponents=use_components,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            logProgress=log_progress,
            username=username,
            concurrency=concurrency,
            jobId=job_id,
            writeConcurrency=write_concurrency,
        )

        result = self._endpoints_helper.run_job_and_write(
            "v2/similarity.nodeSimilarity.filtered",
            G,
            config,
            relationship_type_overwrite=write_relationship_type,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )

        return NodeSimilarityWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        source_node_filter: str | list[int],
        target_node_filter: str | list[int],
        top_k: int = 10,
        bottom_k: int = 10,
        top_n: int = 0,
        bottom_n: int = 0,
        similarity_cutoff: float = 1.0e-42,
        degree_cutoff: int = 1,
        upper_degree_cutoff: int = 2147483647,
        similarity_metric: str = "JACCARD",
        use_components: bool | str = False,
        relationship_weight_property: str | None = None,
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        username: str | None = None,
        concurrency: int | None = None,
    ) -> EstimationResult:
        config = self._endpoints_helper.create_estimate_config(
            sourceNodeFilter=source_node_filter,
            targetNodeFilter=target_node_filter,
            topK=top_k,
            bottomK=bottom_k,
            topN=top_n,
            bottomN=bottom_n,
            similarityCutoff=similarity_cutoff,
            degreeCutoff=degree_cutoff,
            upperDegreeCutoff=upper_degree_cutoff,
            similarityMetric=similarity_metric,
            useComponents=use_components,
            relationshipWeightProperty=relationship_weight_property,
            relationshipTypes=relationship_types,
            nodeLabels=node_labels,
            sudo=sudo,
            username=username,
            concurrency=concurrency,
        )

        return self._endpoints_helper.estimate("v2/similarity.nodeSimilarity.filtered.estimate", G, config)
