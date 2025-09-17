from contextlib import contextmanager
from typing import Any, Generator, Optional, Tuple

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize_single
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.procedure_surface.api.catalog.graph_api import Graph
from graphdatascience.procedure_surface.arrow.catalog.graph_backend_arrow import wrap_graph
from graphdatascience.procedure_surface.arrow.catalog_arrow_endpoints import CatalogArrowEndpoints


@contextmanager
def create_graph(
    arrow_client: AuthenticatedArrowClient, graph_name: str, gdl: str, undirected: Optional[Tuple[str, str]] = None
) -> Generator[Graph, Any, None]:
    try:
        raw_res = arrow_client.do_action("v2/graph.fromGDL", {"graphName": graph_name, "gdlGraph": gdl})
        deserialize_single(raw_res)

        if undirected is not None:
            JobClient.run_job_and_wait(
                arrow_client,
                "v2/graph.relationships.toUndirected",
                {"graphName": graph_name, "relationshipType": undirected[0], "mutateRelationshipType": undirected[1]},
            )

            raw_res = arrow_client.do_action(
                "v2/graph.relationships.drop",
                {"graphName": graph_name, "relationshipType": undirected[0]},
            )
            deserialize_single(raw_res)

        yield wrap_graph(graph_name, arrow_client)
    finally:
        CatalogArrowEndpoints(arrow_client).drop(graph_name, fail_if_missing=False)


@contextmanager
def create_graph_from_db(
    arrow_client: AuthenticatedArrowClient,
    query_runner: QueryRunner,
    graph_name: str,
    graph_data: str,
    query: str,
) -> Generator[Graph, Any, None]:
    try:
        query_runner.run_cypher(graph_data)
        result = CatalogArrowEndpoints(arrow_client, query_runner).project(
            graph_name=graph_name,
            query=query,
        )

        yield result.graph
    finally:
        CatalogArrowEndpoints(arrow_client).drop(graph_name, fail_if_missing=False)
        query_runner.run_cypher("MATCH (n) DETACH DELETE n")
