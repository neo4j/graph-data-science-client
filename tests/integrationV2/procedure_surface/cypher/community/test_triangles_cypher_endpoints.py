from typing import Generator

import pytest

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.cypher.community.triangles_cypher_endpoints import (
    TrianglesCypherEndpoints,
)
from graphdatascience.query_runner import QueryRunner
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_statement = """
    CREATE
    (a: Node),
    (b: Node),
    (c: Node),
    (d: Node),
    (e: Node),
    (f: Node),
    (a)-[:REL]->(b),
    (a)-[:REL]->(c),
    (b)-[:REL]->(c),
    (d)-[:REL]->(e),
    (d)-[:REL]->(f),
    (e)-[:REL]->(f),
    (a)-[:REL]->(d)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {relationshipType: "REL"}, {undirectedRelationshipTypes: ["REL"]}) AS G
        RETURN G
    """

    with create_graph(
        query_runner,
        "g",
        create_statement,
        projection_query,
    ) as g:
        yield g


def test_triangles_happy_path(query_runner: QueryRunner, sample_graph: Graph) -> None:
    result_df = TrianglesCypherEndpoints(query_runner)(G=sample_graph)

    assert list(result_df.columns) == ["nodeA", "nodeB", "nodeC"]
    assert len(result_df) > 0
