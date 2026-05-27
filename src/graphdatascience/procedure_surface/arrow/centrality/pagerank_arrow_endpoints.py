from typing import Any

from pandas import DataFrame
from pydantic import BaseModel

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.scaler_config import ScalerConfig
from graphdatascience.procedure_surface.api.centrality.pagerank_endpoints import (
    PageRankEndpoints,
    PageRankMutateResult,
    PageRankStatsResult,
    PageRankWriteResult,
)
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.arrow.node_property_endpoints import NodePropertyEndpointsHelper
from graphdatascience.query_runner.protocol.write_protocols import WriteProtocol


class PageRankArrowEndpoints(PageRankEndpoints):
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_protocol: WriteProtocol | None = None,
        show_progress: bool = False,
    ):
        self._node_property_endpoints = NodePropertyEndpointsHelper(
            arrow_client, write_protocol, show_progress=show_progress
        )

    def compute(
        self,
        G: GraphV2,
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: int | list[int] | list[tuple[int, float]] | None = None,
    ) -> JobHandle:
        """
        Starts the PageRank algorithm and returns a a :class:`JobHandle`.

        The PageRank algorithm measures the importance of each node within the graph, based on the number of incoming relationships and the importance of the corresponding source nodes.
        The underlying assumption roughly speaking is that a page is only as important as the pages that link to it.

        Parameters
        ----------
        G
           Graph object to use
            Name of the node property to store the results in.
        damping_factor : float
            Probability of a jump to a random node.
        tolerance
            Minimum change in scores between iterations.
        max_iterations : int
            Maximum number of iterations to run.
        scaler
            The scaler to use. Can be:

            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center', 'NONE')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - - A :class:`~graphdatascience.procedure_surface.api.catalog.scaler_config.ScalerConfig` instance
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo
            Disable the memory guard.
        log_progress
            Display progress logging.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.
        relationship_weight_property
            Name of the property to be used as weights.
        source_nodes : int | list[int] | list[tuple[int, float]] | None, default=None
            node ids to use as starting points. Can be:
            - single node id (e.g., 42)
            - list of node id (e.g., [42, 43, 44])
            - list of tuples to associate each node with a bias > 0 (e.g., [(42, 0.5), (43, 1.0)])

        Returns
        -------
        JobHandle
            Used to retrieve the status and results of the computation.
        """

        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            damping_factor=damping_factor,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            scaler=scaler_value,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
        )

        return self._node_property_endpoints.run_job(G, "v2/centrality.pageRank", config)

    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: int | list[int] | list[tuple[int, float]] | None = None,
    ) -> PageRankMutateResult:
        # Convert ScalerConfig to dict if needed
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            damping_factor=damping_factor,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            scaler=scaler_value,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
        )

        result = self._node_property_endpoints.run_job_and_mutate("v2/centrality.pageRank", config, mutate_property)

        return PageRankMutateResult(**result)

    def stats(
        self,
        G: GraphV2,
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: int | list[int] | list[tuple[int, float]] | None = None,
    ) -> PageRankStatsResult:
        # Convert ScalerConfig to dict if needed
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            damping_factor=damping_factor,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            scaler=scaler_value,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary("v2/centrality.pageRank", config)

        return PageRankStatsResult(**computation_result)

    def stream(
        self,
        G: GraphV2,
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: int | list[int] | list[tuple[int, float]] | None = None,
    ) -> DataFrame:
        # Convert ScalerConfig to dict if needed
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            damping_factor=damping_factor,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            scaler=scaler_value,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/centrality.pageRank", G, config)

    def write(
        self,
        G: GraphV2,
        write_property: str,
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: int | list[int] | list[tuple[int, float]] | None = None,
        write_concurrency: int | None = None,
    ) -> PageRankWriteResult:
        # Convert ScalerConfig to dict if needed
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            damping_factor=damping_factor,
            job_id=job_id,
            log_progress=log_progress,
            max_iterations=max_iterations,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            scaler=scaler_value,
            source_nodes=source_nodes,
            sudo=sudo,
            tolerance=tolerance,
        )

        result = self._node_property_endpoints.run_job_and_write(
            "v2/centrality.pageRank",
            G,
            config,
            property_overwrites=write_property,
            write_concurrency=write_concurrency,
            concurrency=concurrency,
        )
        return PageRankWriteResult(**result)

    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        damping_factor: float = 0.85,
        tolerance: float = 1.0e-7,
        max_iterations: int = 20,
        scaler: str | dict[str, str | int | float] | ScalerConfig = "NONE",
        relationship_types: list[str] = ALL_TYPES,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
        relationship_weight_property: str | None = None,
        source_nodes: int | list[int] | list[tuple[int, float]] | None = None,
    ) -> EstimationResult:
        # Convert ScalerConfig to dict if needed
        scaler_value = scaler.model_dump() if isinstance(scaler, BaseModel) else scaler

        config = self._node_property_endpoints.create_estimate_config(
            damping_factor=damping_factor,
            tolerance=tolerance,
            max_iterations=max_iterations,
            scaler=scaler_value,
            relationship_types=relationship_types,
            node_labels=node_labels,
            concurrency=concurrency,
            relationship_weight_property=relationship_weight_property,
            source_nodes=source_nodes,
        )
        return self._node_property_endpoints.estimate("v2/centrality.pageRank.estimate", G, config)
