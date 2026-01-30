from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.centrality.betweenness_cypher_endpoints import BetweennessCypherEndpoints
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node),
    (b: Node),
    (c: Node),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {}) AS G
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
def betweenness_endpoints(query_runner: QueryRunner) -> Generator[BetweennessCypherEndpoints, None, None]:
    yield BetweennessCypherEndpoints(query_runner)


def test_betweenness_stats(betweenness_endpoints: BetweennessCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Betweenness Centrality stats operation."""
    result = betweenness_endpoints.stats(
        G=sample_graph,
    )

    assert "p50" in result.centrality_distribution
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0


def test_betweenness_stream(betweenness_endpoints: BetweennessCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Betweenness Centrality stream operation."""
    result = betweenness_endpoints.stream(
        G=sample_graph,
    )

    assert len(result) == 3  # Should have 3 nodes
    assert "nodeId" in result.columns
    assert "score" in result.columns


def test_betweenness_mutate(betweenness_endpoints: BetweennessCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Betweenness Centrality mutate operation."""
    result = betweenness_endpoints.mutate(
        G=sample_graph,
        mutate_property="betweenness",
    )

    assert "p50" in result.centrality_distribution
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 3


def test_betweenness_write(betweenness_endpoints: BetweennessCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Betweenness Centrality write operation."""
    result = betweenness_endpoints.write(
        G=sample_graph,
        write_property="betweenness",
    )

    assert "p50" in result.centrality_distribution
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 3


def test_betweenness_estimate(betweenness_endpoints: BetweennessCypherEndpoints, sample_graph: GraphV2) -> None:
    result = betweenness_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 2
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
