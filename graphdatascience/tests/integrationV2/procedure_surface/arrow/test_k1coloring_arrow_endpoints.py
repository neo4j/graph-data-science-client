from typing import Generator

import pytest

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.k1coloring_arrow_endpoints import K1ColoringArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    (a: Node)
    (b: Node)
    (c: Node)
    (a)-[:REL]->(b)
    (a)-[:REL]->(c)
    (b)-[:REL]->(c)
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def k1coloring_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[K1ColoringArrowEndpoints, None, None]:
    yield K1ColoringArrowEndpoints(arrow_client)


def test_k1coloring_stats(k1coloring_endpoints: K1ColoringArrowEndpoints, sample_graph: Graph) -> None:
    """Test K1Coloring stats operation."""
    result = k1coloring_endpoints.stats(G=sample_graph)

    assert result.color_count == 3
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.ran_iterations >= 1
    assert result.did_converge
    assert isinstance(result.did_converge, bool)


def test_k1coloring_stream(k1coloring_endpoints: K1ColoringArrowEndpoints, sample_graph: Graph) -> None:
    """Test K1Coloring stream operation."""
    result_df = k1coloring_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "color" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 3


def test_k1coloring_mutate(k1coloring_endpoints: K1ColoringArrowEndpoints, sample_graph: Graph) -> None:
    """Test K1Coloring mutate operation."""
    result = k1coloring_endpoints.mutate(
        G=sample_graph,
        mutate_property="color",
    )

    assert result.color_count == 3
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_count == 3
    assert result.ran_iterations >= 1
    assert result.did_converge
    assert isinstance(result.did_converge, bool)


def test_k1coloring_estimate(k1coloring_endpoints: K1ColoringArrowEndpoints, sample_graph: Graph) -> None:
    result = k1coloring_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 3
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
