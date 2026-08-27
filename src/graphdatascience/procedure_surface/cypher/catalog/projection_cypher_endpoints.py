from __future__ import annotations

import builtins
from types import TracebackType
from typing import Any, NamedTuple, Type

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult
from graphdatascience.procedure_surface.cypher.catalog.graph_backend_cypher import get_graph
from graphdatascience.procedure_surface.cypher.catalog.graph_ops_cypher import GraphOpsCypher
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner import QueryRunner
from graphdatascience.query_runner.query_mode import QueryMode
from graphdatascience.query_runner.query_type import QueryType


class GraphProjectResult(BaseResult):
    graph_name: str
    node_count: int
    relationship_count: int
    project_millis: int
    node_projection: dict[str, Any]
    relationship_projection: dict[str, Any]


class GraphWithProjectResult(NamedTuple):
    graph: Graph
    result: GraphProjectResult

    def __enter__(self) -> Graph:
        return self.graph

    def __exit__(
        self,
        exception_type: Type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.graph.drop()


class GraphCypherProjectResult(BaseResult):
    graph_name: str
    node_count: int
    relationship_count: int
    project_millis: int
    configuration: dict[str, Any]
    query: str


class GraphWithCypherProjectResult(NamedTuple):
    graph: Graph
    result: GraphCypherProjectResult

    def __enter__(self) -> Graph:
        return self.graph

    def __exit__(
        self,
        exception_type: Type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.graph.drop()


class ProjectCypherEndpoints:
    """Endpoints for projecting a graph into the catalog against a Cypher-based database.

    Exposes :meth:`native` for ``gds.graph.project.native``, :meth:`estimate` for
    ``gds.graph.project.estimate`` and :meth:`cypher` for ``gds.graph.project.cypher``.
    """

    def __init__(self, cypher_runner: QueryRunner):
        self._cypher_runner = cypher_runner
        self._graph_ops = GraphOpsCypher(cypher_runner)

    def native(
        self,
        graph_name: str,
        node_projection: str | builtins.list[str] | dict[str, Any] | None = None,
        relationship_projection: str | builtins.list[str] | dict[str, Any] | None = None,
        node_properties: str | builtins.list[str] | dict[str, Any] | None = None,
        relationship_properties: str | builtins.list[str] | dict[str, Any] | None = None,
        read_concurrency: int | None = None,
        job_id: str | None = None,
        sudo: bool = False,
        username: str | None = None,
        log_progress: bool = True,
        overwrite: bool = False,
    ) -> GraphWithProjectResult:
        """Project a graph into the catalog using a native projection (``gds.graph.project``)."""
        if overwrite:
            self._graph_ops.drop(graph_name, fail_if_missing=False, username=username)

        config = ConfigConverter.convert_to_gds_config(
            nodeProperties=node_properties,
            relationshipProperties=relationship_properties,
            jobId=job_id,
            sudo=sudo,
            username=username,
            readConcurrency=read_concurrency,
        )

        params = CallParameters(
            graphName=graph_name,
            nodeProjection=node_projection,
            relationshipProjection=relationship_projection,
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._cypher_runner.call_procedure(
            endpoint="gds.graph.project", params=params, logging=log_progress
        ).iloc[0]
        project_result = GraphProjectResult(**result)
        return GraphWithProjectResult(get_graph(project_result.graph_name, self._cypher_runner), project_result)

    def estimate(
        self,
        node_projection: str | builtins.list[str] | dict[str, Any] | None = None,
        relationship_projection: str | builtins.list[str] | dict[str, Any] | None = None,
        node_properties: str | builtins.list[str] | dict[str, Any] | None = None,
        relationship_properties: str | builtins.list[str] | dict[str, Any] | None = None,
        read_concurrency: int | None = None,
        sudo: bool = False,
        username: str | None = None,
    ) -> EstimationResult:
        """Estimate the memory consumption of a native graph projection.

        Parameters
        ----------
        node_projection
            The node projection used for the projection. A single label, a list of labels, or a projection map.
        relationship_projection
            The relationship projection used for the projection. A single type, a list of types, or a projection map.
        node_properties
            Node properties to load during the projection.
        relationship_properties
            Relationship properties to load during the projection.
        read_concurrency
            Number of concurrent threads used during the projection.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for the estimation.

        Returns
        -------
        EstimationResult
            The result of the estimation, including the required memory and node/relationship counts.
        """
        config = ConfigConverter.convert_to_gds_config(
            nodeProperties=node_properties,
            relationshipProperties=relationship_properties,
            sudo=sudo,
            username=username,
            readConcurrency=read_concurrency,
        )

        params = CallParameters(
            nodeProjection=node_projection,
            relationshipProjection=relationship_projection,
            config=config,
        )

        result = self._cypher_runner.call_procedure(endpoint="gds.graph.project.estimate", params=params).iloc[0]
        return EstimationResult(**result)

    def cypher(
        self,
        query: str,
        **params: Any,
    ) -> GraphWithCypherProjectResult:
        """Project a graph using a Cypher projection.

        The provided query must end with a ``RETURN gds.graph.project(...)`` aggregation call.

        Parameters
        ----------
        query
            The Cypher projection query. Must end with a ``RETURN gds.graph.project(...)`` call.
        **params
            Query parameters referenced in the Cypher query.

        Returns
        -------
        GraphWithCypherProjectResult
            The projected graph and metadata about the projection.
        """
        result_df = self._cypher_runner.run_retryable_cypher(
            query, QueryType.USER_DIRECTED, params, custom_error=False, mode=QueryMode.READ
        )

        if result_df.empty:
            raise ValueError(
                "The Cypher projection query produced no rows. "
                "Please check that the query matches the expected data. "
                f"Query: {query}"
            )

        result = result_df.squeeze()

        if not isinstance(result, dict):
            raise ValueError(
                "The Cypher projection query must end with a single `RETURN gds.graph.project(...)` "
                f"call, but got: {query}"
            )

        project_result = GraphCypherProjectResult(**result)
        return GraphWithCypherProjectResult(get_graph(project_result.graph_name, self._cypher_runner), project_result)
