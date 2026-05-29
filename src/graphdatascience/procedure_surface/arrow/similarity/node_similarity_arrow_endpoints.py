from __future__ import annotations

from typing import Any

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.similarity.node_similarity_endpoints import NodeSimilarityEndpoints
from graphdatascience.procedure_surface.api.similarity.node_similarity_filtered_endpoints import (
    NodeSimilarityFilteredEndpoints,
)
from graphdatascience.procedure_surface.api.similarity.node_similarity_results import (
    NodeSimilarityMutateResult,
    NodeSimilarityStatsResult,
    NodeSimilarityWriteResult,
)
from graphdatascience.procedure_surface.arrow.relationship_endpoints_helper import RelationshipEndpointsHelper
from graphdatascience.procedure_surface.arrow.similarity.node_similarity_filtered_arrow_endpoints import (
    NodeSimilarityFilteredArrowEndpoints,
)
from graphdatascience.procedure_surface.arrow.stream_result_mapper import rename_similarity_stream_result
from graphdatascience.query_runner.protocol.write_protocols import WriteProtocol


class NodeSimilarityArrowEndpoints(NodeSimilarityEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = False,
    ):
        self._endpoints_helper = RelationshipEndpointsHelper(
            arrow_client, write_protocol=write_protocol, show_progress=show_progress
        )

    @property
    def filtered(self) -> NodeSimilarityFilteredEndpoints:
        return NodeSimilarityFilteredArrowEndpoints(
            self._endpoints_helper._arrow_client,
            self._endpoints_helper._write_protocol,
            self._endpoints_helper._show_progress,
        )

    def compute(
        self,
        G: GraphV2,
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
    ) -> JobHandle:
        """
        Kick off a non-blocking NodeSimilarity computation and return a :class:`JobHandle`.

        Parameters
        ----------
        G
           Graph object to use
        top_k
            Number of most similar nodes to return for each node.
        bottom_k : int, default=10
            The maximum number of neighbors with the lowest similarity scores to compute per node.
        top_n : int, default=0
            The maximum number of neighbors to select globally based on similarity scores.
        bottom_n : int, default=0
            The maximum number of neighbors to select globally based on lowest similarity scores.
        similarity_cutoff
            The threshold for similarity scores.
        degree_cutoff : int, default=1
            The minimum degree a node must have to be considered.
        upper_degree_cutoff : int, default=2147483647
            The maximum degree a node can have to be considered.
        similarity_metric : str, default="JACCARD"
            The similarity metric to use for computation.
        use_components : bool | str, default=False
            Whether to compute similarity within connected components. Given a string uses the node property stored in the graph
        relationship_weight_property
            Name of the property to be used as weights.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

        Returns
        -------
        JobHandle
            Non-blocking handle to the running computation.
        """

        config = self._endpoints_helper.create_base_config(
            G,
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
        return self._endpoints_helper.run_job(G, "v2/similarity.nodeSimilarity", config)

    def mutate(
        self,
        G: GraphV2,
        mutate_relationship_type: str,
        mutate_property: str,
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
            "v2/similarity.nodeSimilarity", config, mutate_property, mutate_relationship_type
        )

        return NodeSimilarityMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
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
            "v2/similarity.nodeSimilarity",
            config,
        )
        if "similarityPairs" not in result:
            result["similarityPairs"] = result.get("relationshipsWritten", 0)

        return NodeSimilarityStatsResult(**result)

    def stream(
        self,
        G: GraphV2,
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

        result = self._endpoints_helper.run_job_and_stream("v2/similarity.nodeSimilarity", G, config)

        rename_similarity_stream_result(result)
        return result

    def write(
        self,
        G: GraphV2,
        write_relationship_type: str,
        write_property: str,
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
            "v2/similarity.nodeSimilarity",
            G,
            config,
            property_overwrites=write_property,
            relationship_type_overwrite=write_relationship_type,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )

        return NodeSimilarityWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
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

        return self._endpoints_helper.estimate("v2/similarity.nodeSimilarity.estimate", G, config)
