from typing import Generator

import pytest

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.closeness_arrow_endpoints import ClosenessArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    (a: Node)
    (b: Node)
    (c: Node)
    (d: Node)
    (a)-[:REL]->(b)
    (b)-[:REL]->(c)
    (c)-[:REL]->(d)
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def closeness_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[ClosenessArrowEndpoints, None, None]:
    yield ClosenessArrowEndpoints(arrow_client)


def test_closeness_stats(closeness_endpoints: ClosenessArrowEndpoints, sample_graph: Graph) -> None:
    """Test Closeness stats operation."""
    result = closeness_endpoints.stats(G=sample_graph)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert "p50" in result.centrality_distribution


def test_closeness_stream(closeness_endpoints: ClosenessArrowEndpoints, sample_graph: Graph) -> None:
    """Test Closeness stream operation."""
    result_df = closeness_endpoints.stream(
        G=sample_graph,
    )

    assert set(result_df.columns) == {"nodeId", "score"}
    assert len(result_df) == 4


def test_closeness_mutate(closeness_endpoints: ClosenessArrowEndpoints, sample_graph: Graph) -> None:
    """Test Closeness mutate operation."""
    result = closeness_endpoints.mutate(
        G=sample_graph,
        mutate_property="closeness",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 4
    assert "p50" in result.centrality_distribution


def test_closeness_estimate(closeness_endpoints: ClosenessArrowEndpoints, sample_graph: Graph) -> None:
    result = closeness_endpoints.estimate(sample_graph)

    assert result.node_count == 4
    assert result.relationship_count == 3
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0


def test_closeness_with_wasserman_faust(closeness_endpoints: ClosenessArrowEndpoints, sample_graph: Graph) -> None:
    """Test Closeness with Wasserman-Faust normalization."""
    result = closeness_endpoints.stats(
        G=sample_graph,
        use_wasserman_faust=True,
    )

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert "p50" in result.centrality_distribution
