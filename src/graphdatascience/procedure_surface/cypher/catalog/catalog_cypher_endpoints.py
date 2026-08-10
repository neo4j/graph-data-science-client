from __future__ import annotations

from typing import Any, cast

from pandas import DataFrame

from graphdatascience.arrow_client.v1.gds_arrow_client import GdsArrowClient
from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.graph_api import Graph
from graphdatascience.graph.graph_info import GraphInfo, GraphInfoWithDegrees
from graphdatascience.graph_construction.arrow_v1_graph_constructor import ArrowV1GraphConstructor
from graphdatascience.graph_construction.cypher_graph_constructor import CypherGraphConstructor
from graphdatascience.graph_construction.graph_constructor import GraphConstructor
from graphdatascience.procedure_surface.api.catalog import (
    NodeLabelEndpoints,
    NodePropertiesEndpoints,
    RelationshipsEndpoints,
)
from graphdatascience.procedure_surface.api.catalog.catalog_endpoints import (
    CatalogEndpoints,
    GraphFilterResult,
    GraphGenerationStats,
    GraphWithFilterResult,
    GraphWithGenerationStats,
    RelationshipPropertySpec,
    validate_distinct_from_source,
)
from graphdatascience.procedure_surface.api.catalog.graph_export_endpoints import GraphExportEndpoints
from graphdatascience.procedure_surface.api.catalog.graph_sampling_endpoints import GraphSamplingEndpoints
from graphdatascience.procedure_surface.cypher.catalog.graph_backend_cypher import get_graph
from graphdatascience.procedure_surface.cypher.catalog.graph_export_cypher_endpoints import (
    GraphExportCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.catalog.graph_sampling_cypher_endpoints import (
    GraphSamplingCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.catalog.node_label_cypher_endpoints import NodeLabelCypherEndpoints
from graphdatascience.procedure_surface.cypher.catalog.node_properties_cypher_endpoints import (
    NodePropertiesCypherEndpoints,
)
from graphdatascience.procedure_surface.cypher.catalog.projection_cypher_endpoints import ProjectCypherEndpoints
from graphdatascience.procedure_surface.cypher.catalog.relationship_cypher_endpoints import RelationshipCypherEndpoints
from graphdatascience.procedure_surface.cypher.catalog.utils import (
    GRAPH_INFO_WITH_DEGREES_YIELDS,
    GRAPH_INFO_YIELDS,
    drop_graph_if_exists,
    require_database,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner import QueryRunner
from graphdatascience.query_runner.query_mode import QueryMode


class CatalogCypherEndpoints(CatalogEndpoints):
    def __init__(self, cypher_runner: QueryRunner, arrow_client: GdsArrowClient | None = None):
        self._cypher_runner = cypher_runner
        self._arrow_client = arrow_client

    def get(self, graph_name: str) -> Graph:
        if not self.list(graph_name):
            raise ValueError(f"A graph with name '{graph_name}' does not exist in the catalog.")
        return get_graph(graph_name, self._cypher_runner)

    def exists(self, graph_name: str) -> bool:
        return cast(
            bool,
            self._cypher_runner.call_function(endpoint="gds.graph.exists", params=CallParameters(graphName=graph_name)),
        )

    def construct(
        self,
        graph_name: str,
        nodes: DataFrame | list[DataFrame],
        relationships: DataFrame | list[DataFrame] | None = None,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        batch_size: int = 100000,
        overwrite: bool = False,
    ) -> Graph:
        if overwrite:
            drop_graph_if_exists(self._cypher_runner, graph_name)

        if isinstance(nodes, DataFrame):
            nodes = [nodes]
        if relationships is None:
            relationships = []
        elif isinstance(relationships, DataFrame):
            relationships = [relationships]

        graph_constructor: GraphConstructor
        if self._arrow_client is not None:
            database = require_database(self._cypher_runner)

            graph_constructor = ArrowV1GraphConstructor(
                database=database,
                graph_name=graph_name,
                flight_client=self._arrow_client,
                concurrency=concurrency,
                undirected_relationship_types=undirected_relationship_types,
                inverse_indexed_relationship_types=inverse_indexed_relationship_types,
                batch_size=batch_size,
            )
        else:
            graph_constructor = CypherGraphConstructor(
                query_runner=self._cypher_runner,
                graph_name=graph_name,
                concurrency=concurrency,
                undirected_relationship_types=undirected_relationship_types,
                inverse_indexed_relationship_types=inverse_indexed_relationship_types,
            )

        graph_constructor.run(node_dfs=nodes, relationship_dfs=relationships)
        return get_graph(graph_name, self._cypher_runner)

    def list(self, G: Graph | str | None = None) -> list[GraphInfoWithDegrees]:
        graph_name = G if isinstance(G, str) else G.name() if G is not None else None
        params = CallParameters(graphName=graph_name) if graph_name else CallParameters()

        result = self._cypher_runner.call_procedure(
            endpoint="gds.graph.list", params=params, yields=GRAPH_INFO_WITH_DEGREES_YIELDS
        )
        return [GraphInfoWithDegrees(**row) for _, row in result.iterrows()]

    def drop(
        self,
        G: Graph | str,
        fail_if_missing: bool = True,
        *,
        db_name: str | None = None,
        username: str | None = None,
    ) -> GraphInfo | None:
        """Drop a graph from the graph catalog.

        Parameters
        ----------
        G
            Graph to drop by name or object.
        fail_if_missing
            Whether to fail if the graph is missing.
        db_name
            The name of the database the graph belongs to. Defaults to the current database.
        username
            As an administrator, drop a graph owned by a different user.

        Returns
        -------
        GraphInfo | None
            Metadata of the dropped graph, or None if the graph did not exist.
        """
        graph_name = G if isinstance(G, str) else G.name()

        params = CallParameters(graphName=graph_name, failIfMissing=fail_if_missing)

        if db_name is not None or username is not None:
            # positional params. order has to be preserved
            params["dbName"] = db_name if db_name is not None else ""
            params["username"] = username if username is not None else ""

        result = self._cypher_runner.call_procedure(
            endpoint="gds.graph.drop",
            params=params,
            yields=GRAPH_INFO_YIELDS,
            # dropping is idempotent as long as a missing graph is not an error
            retryable=not fail_if_missing,
            mode=QueryMode.WRITE,
        )
        if len(result) > 0:
            return GraphInfo(**result.iloc[0])
        else:
            return None

    @property
    def project(self) -> ProjectCypherEndpoints:
        return ProjectCypherEndpoints(self._cypher_runner)

    @property
    def export(self) -> GraphExportEndpoints:
        return GraphExportCypherEndpoints(self._cypher_runner)

    def filter(
        self,
        G: Graph,
        graph_name: str,
        node_filter: str,
        relationship_filter: str,
        parameters: dict[str, Any] | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        overwrite: bool = False,
    ) -> GraphWithFilterResult:
        validate_distinct_from_source(graph_name, G)
        if overwrite:
            drop_graph_if_exists(self._cypher_runner, graph_name)

        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            jobId=job_id,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
            parameters=parameters,
        )

        params = CallParameters(
            graph_name=graph_name,
            from_graph_name=G.name(),
            node_filter=node_filter,
            relationship_filter=relationship_filter,
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._cypher_runner.call_procedure(
            endpoint="gds.graph.filter", params=params, logging=log_progress
        ).iloc[0]
        return GraphWithFilterResult(get_graph(graph_name, self._cypher_runner), GraphFilterResult(**result))

    def generate(
        self,
        graph_name: str,
        node_count: int,
        average_degree: float,
        *,
        relationship_distribution: str | None = "UNIFORM",
        relationship_seed: int | None = None,
        relationship_property: RelationshipPropertySpec | None = None,
        orientation: str | None = "NATURAL",
        aggregation: str | None = "NONE",
        allow_self_loops: bool | None = False,
        read_concurrency: int | None = None,
        job_id: str | None = None,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        overwrite: bool = False,
    ) -> GraphWithGenerationStats:
        if overwrite:
            drop_graph_if_exists(self._cypher_runner, graph_name)

        config = ConfigConverter.convert_to_gds_config(
            relationship_distribution=relationship_distribution,
            relationship_seed=relationship_seed,
            relationship_property=relationship_property.model_dump(by_alias=True) if relationship_property else None,
            orientation=orientation,
            aggregation=aggregation,
            allow_self_loops=allow_self_loops,
            read_concurrency=read_concurrency,
            job_id=job_id,
            sudo=sudo,
            log_progress=log_progress,
            username=username,
        )

        params = CallParameters(
            graph_name=graph_name,
            node_count=node_count,
            average_degree=average_degree,
            config=config,
        )

        params.ensure_job_id_in_config()

        result = self._cypher_runner.call_procedure(
            endpoint="gds.graph.generate", params=params, logging=log_progress
        ).iloc[0]
        return GraphWithGenerationStats(get_graph(graph_name, self._cypher_runner), GraphGenerationStats(**result))

    @property
    def sample(self) -> GraphSamplingEndpoints:
        return GraphSamplingCypherEndpoints(self._cypher_runner)

    @property
    def node_labels(self) -> NodeLabelEndpoints:
        return NodeLabelCypherEndpoints(self._cypher_runner)

    @property
    def node_properties(self) -> NodePropertiesEndpoints:
        return NodePropertiesCypherEndpoints(self._cypher_runner, self._arrow_client)

    @property
    def relationships(self) -> RelationshipsEndpoints:
        return RelationshipCypherEndpoints(self._cypher_runner, self._arrow_client)
