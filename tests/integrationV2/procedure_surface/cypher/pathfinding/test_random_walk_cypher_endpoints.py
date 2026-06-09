from typing import Generator

import pytest

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.pathfinding.random_walk_cypher_endpoints import (
    RandomWalkCypherEndpoints,
)
from graphdatascience.query_runner import QueryRunner
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node {id: 0}),
    (b: Node {id: 1}),
    (c: Node {id: 2}),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(a)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m) AS G
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
def random_walk_endpoints(query_runner: QueryRunner) -> Generator[RandomWalkCypherEndpoints, None, None]:
    yield RandomWalkCypherEndpoints(query_runner)


def test_random_walk_stream(random_walk_endpoints: RandomWalkCypherEndpoints, sample_graph: GraphV2) -> None:
    result_df = random_walk_endpoints.stream(G=sample_graph, walks_per_node=2, walk_length=4, random_seed=42)

    assert len(result_df) > 0


def test_random_walk_mutate(random_walk_endpoints: RandomWalkCypherEndpoints, sample_graph: GraphV2) -> None:
    result = random_walk_endpoints.mutate(
        G=sample_graph, mutate_property="walks", walks_per_node=1, walk_length=3, random_seed=42
    )

    assert result.node_properties_written > 0


def test_random_walk_stats(random_walk_endpoints: RandomWalkCypherEndpoints, sample_graph: GraphV2) -> None:
    result = random_walk_endpoints.stats(G=sample_graph, walks_per_node=1, walk_length=3, random_seed=42)

    assert result.compute_millis >= 0


def test_random_walk_estimate(random_walk_endpoints: RandomWalkCypherEndpoints, sample_graph: GraphV2) -> None:
    result = random_walk_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 3
    assert "Bytes" in result.required_memory
