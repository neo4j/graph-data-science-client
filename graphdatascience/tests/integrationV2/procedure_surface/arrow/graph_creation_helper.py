from contextlib import contextmanager
from typing import Any, Generator

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize_single
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.catalog.graph_backend_arrow import get_graph
from graphdatascience.procedure_surface.arrow.catalog_arrow_endpoints import CatalogArrowEndpoints


@contextmanager
def create_graph(
    arrow_client: AuthenticatedArrowClient, graph_name: str, gdl: str, undirected: tuple[str, str] | None = None
) -> Generator[GraphV2, Any, None]:
    try:
        raw_res = list(arrow_client.do_action("v2/graph.fromGDL", {"graphName": graph_name, "gdlGraph": gdl}))

        if undirected is not None:
            JobClient.run_job_and_wait(
                arrow_client,
                "v2/graph.relationships.toUndirected",
                {"graphName": graph_name, "relationshipType": undirected[0], "mutateRelationshipType": undirected[1]},
                show_progress=False,
            )

            raw_res = list(
                arrow_client.do_action(
                    "v2/graph.relationships.drop",
                    {"graphName": graph_name, "relationshipType": undirected[0]},
                )
            )
            deserialize_single(raw_res)

        yield get_graph(graph_name, arrow_client)
    finally:
        CatalogArrowEndpoints(arrow_client).drop(graph_name, fail_if_missing=False)


@contextmanager
def create_graph_from_db(
    arrow_client: AuthenticatedArrowClient,
    query_runner: QueryRunner,
    graph_name: str,
    graph_data: str,
    query: str,
    undirected_relationship_types: list[str] | None = None,
) -> Generator[GraphV2, Any, None]:
    try:
        query_runner.run_cypher(graph_data)
        result = CatalogArrowEndpoints(arrow_client, query_runner).project(
            graph_name=graph_name,
            query=query,
            undirected_relationship_types=undirected_relationship_types,
        )

        yield result.graph
    finally:
        CatalogArrowEndpoints(arrow_client).drop(graph_name, fail_if_missing=False)
        query_runner.run_cypher("MATCH (n) DETACH DELETE n")
