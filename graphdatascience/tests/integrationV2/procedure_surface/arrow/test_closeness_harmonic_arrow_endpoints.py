from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.closeness_harmonic_arrow_endpoints import ClosenessHarmonicArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
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
def closeness_harmonic_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[ClosenessHarmonicArrowEndpoints, None, None]:
    yield ClosenessHarmonicArrowEndpoints(arrow_client)


def test_closeness_harmonic_stats(
    closeness_harmonic_endpoints: ClosenessHarmonicArrowEndpoints, sample_graph: GraphV2
) -> None:
    """Test Harmonic Closeness stats operation."""
    result = closeness_harmonic_endpoints.stats(G=sample_graph)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert "p50" in result.centrality_distribution


def test_closeness_harmonic_stream(
    closeness_harmonic_endpoints: ClosenessHarmonicArrowEndpoints, sample_graph: GraphV2
) -> None:
    """Test Harmonic Closeness stream operation."""
    result_df = closeness_harmonic_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "score" in result_df.columns
    assert len(result_df.columns) == 2


def test_closeness_harmonic_mutate(
    closeness_harmonic_endpoints: ClosenessHarmonicArrowEndpoints, sample_graph: GraphV2
) -> None:
    """Test Harmonic Closeness mutate operation."""
    result = closeness_harmonic_endpoints.mutate(
        G=sample_graph,
        mutate_property="harmonic_closeness",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 4
    assert "p50" in result.centrality_distribution


def test_closeness_harmonic_estimate(
    closeness_harmonic_endpoints: ClosenessHarmonicArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = closeness_harmonic_endpoints.estimate(sample_graph)

    assert result.node_count == 4
    assert result.relationship_count == 3
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
