from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.catalog.graph_api import Graph
from graphdatascience.procedure_surface.arrow.pagerank_arrow_endpoints import PageRankArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    (a: Node)
    (b: Node)
    (c: Node)
    (a)-[:REL]->(c)
    (b)-[:REL]->(c)
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def pagerank_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[PageRankArrowEndpoints, None, None]:
    yield PageRankArrowEndpoints(arrow_client)


def test_pagerank_stats(pagerank_endpoints: PageRankArrowEndpoints, sample_graph: Graph) -> None:
    """Test PageRank stats operation."""
    result = pagerank_endpoints.stats(G=sample_graph)

    assert result.ran_iterations > 0
    assert result.did_converge
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert "p50" in result.centrality_distribution


def test_pagerank_stream(pagerank_endpoints: PageRankArrowEndpoints, sample_graph: Graph) -> None:
    """Test PageRank stream operation."""
    result_df = pagerank_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "score" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 3  # We have 3 nodes


def test_pagerank_mutate(pagerank_endpoints: PageRankArrowEndpoints, sample_graph: Graph) -> None:
    """Test PageRank mutate operation."""
    result = pagerank_endpoints.mutate(
        G=sample_graph,
        mutate_property="pagerank",
    )

    assert result.ran_iterations > 0
    assert result.did_converge
    assert "p50" in result.centrality_distribution
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 3


def test_pagerank_estimate(pagerank_endpoints: PageRankArrowEndpoints, sample_graph: Graph) -> None:
    result = pagerank_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 2
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
