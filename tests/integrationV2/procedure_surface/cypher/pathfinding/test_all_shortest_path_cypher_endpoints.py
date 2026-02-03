from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.pathfinding.all_shortest_path_cypher_endpoints import (
    AllShortestPathCypherEndpoints,
)
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
        CREATE
              (a:Node)
            , (b:Node)
            , (c:Node)
            , (d:Node)
            , (e:Node)
            , (a)-[:TYPE {cost: 1.0}]->(b)
            , (b)-[:TYPE {cost: 1.0}]->(c)
            , (a)-[:TYPE {cost: 2.0}]->(c)
            , (c)-[:TYPE {cost: 1.0}]->(d)
            , (d)-[:TYPE {cost: 1.0}]->(e)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {relationshipProperties: r {.cost}}) AS G
        RETURN G
    """

    with create_graph(
        query_runner,
        "g",
        create_statement,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def all_shortest_path_endpoints(query_runner: QueryRunner) -> AllShortestPathCypherEndpoints:
    return AllShortestPathCypherEndpoints(query_runner)


def test_all_shortest_paths_stream(
    all_shortest_path_endpoints: AllShortestPathCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = all_shortest_path_endpoints.stream(sample_graph)

    assert len(result) > 0
    assert set(result.columns) == {"sourceNodeId", "targetNodeId", "distance"}


def test_all_shortest_paths_estimate(
    all_shortest_path_endpoints: AllShortestPathCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = all_shortest_path_endpoints.estimate(G=sample_graph)

    assert result.node_count == 5
    assert result.relationship_count == 5
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
