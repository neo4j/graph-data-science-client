from typing import Generator

import pytest

from graphdatascience import Graph, QueryRunner
from graphdatascience.procedure_surface.cypher.articlerank_cypher_endpoints import ArticleRankCypherEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_query = """
        CREATE
        (a: Node),
        (b: Node),
        (c: Node),
        (a)-[:REL]->(c),
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
        create_query,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def articlerank_endpoints(query_runner: QueryRunner) -> Generator[ArticleRankCypherEndpoints, None, None]:
    yield ArticleRankCypherEndpoints(query_runner)


def test_articlerank_stats(articlerank_endpoints: ArticleRankCypherEndpoints, sample_graph: Graph) -> None:
    """Test ArticleRank stats operation."""
    result = articlerank_endpoints.stats(G=sample_graph)

    assert result.ran_iterations > 0
    assert result.did_converge in [True, False]
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert "p50" in result.centrality_distribution or "p10" in result.centrality_distribution


def test_articlerank_stream(articlerank_endpoints: ArticleRankCypherEndpoints, sample_graph: Graph) -> None:
    """Test ArticleRank stream operation."""
    result_df = articlerank_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "score" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 3  # We have 3 nodes


def test_articlerank_mutate(articlerank_endpoints: ArticleRankCypherEndpoints, sample_graph: Graph) -> None:
    """Test ArticleRank mutate operation."""
    result = articlerank_endpoints.mutate(
        G=sample_graph,
        mutate_property="articlerank",
    )

    assert result.ran_iterations > 0
    assert result.did_converge in [True, False]
    assert "p50" in result.centrality_distribution or "p10" in result.centrality_distribution
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 3


def test_articlerank_estimate(articlerank_endpoints: ArticleRankCypherEndpoints, sample_graph: Graph) -> None:
    result = articlerank_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 2
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
