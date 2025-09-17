from contextlib import contextmanager
from typing import Any, Generator

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.catalog.graph_backend_cypher import wrap_graph


@contextmanager
def create_graph(
    query_runner: QueryRunner, graph_name: str, data_query: str, projection_query: str
) -> Generator[GraphV2, Any, None]:
    try:
        query_runner.run_cypher(data_query)
        query_runner.run_cypher(projection_query)
        yield wrap_graph(graph_name, query_runner)
    finally:
        delete_all_graphs(query_runner)
        query_runner.run_cypher("MATCH (n) DETACH DELETE n")


def delete_all_graphs(query_runner: QueryRunner) -> None:
    query_runner.run_cypher(
        "CALL gds.graph.list() YIELD graphName CALL gds.graph.drop(graphName) YIELD graphName as g RETURN g"
    )
