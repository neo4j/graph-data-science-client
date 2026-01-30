from __future__ import annotations

from typing import Any

from pandas import DataFrame

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.v2.graph_api import GraphV2
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
from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class NodeSimilarityFilteredCypherEndpoints(NodeSimilarityFilteredEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

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
        config = ConfigConverter.convert_to_gds_config(
            mutateRelationshipType=mutate_relationship_type,
            mutateProperty=mutate_property,
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
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure("gds.nodeSimilarity.filtered.mutate", params=params).iloc[0]

        return NodeSimilarityMutateResult(**result.to_dict())

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
        config = ConfigConverter.convert_to_gds_config(
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
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            "gds.nodeSimilarity.filtered.stats", params=params, logging=log_progress
        ).iloc[0]

        return NodeSimilarityStatsResult(**result.to_dict())

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
        config = ConfigConverter.convert_to_gds_config(
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
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(
            "gds.nodeSimilarity.filtered.stream", params=params, logging=log_progress
        )

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
        config = ConfigConverter.convert_to_gds_config(
            writeRelationshipType=write_relationship_type,
            writeProperty=write_property,
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
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        result = self._query_runner.call_procedure(
            "gds.nodeSimilarity.filtered.write", params=params, logging=log_progress
        ).iloc[0]

        return NodeSimilarityWriteResult(**result.to_dict())

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
        config = ConfigConverter.convert_to_gds_config(
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

        return estimate_algorithm("gds.nodeSimilarity.filtered.stats.estimate", self._query_runner, G, config)
