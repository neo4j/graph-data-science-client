import json
from typing import Generator

import pytest

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.eigenvector_arrow_endpoints import EigenvectorArrowEndpoints
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
    (d)-[:REL]->(a)
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def eigenvector_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[EigenvectorArrowEndpoints, None, None]:
    yield EigenvectorArrowEndpoints(arrow_client)


def test_eigenvector_stats(eigenvector_endpoints: EigenvectorArrowEndpoints, sample_graph: Graph) -> None:
    """Test Eigenvector stats operation."""
    result = eigenvector_endpoints.stats(G=sample_graph)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.ran_iterations >= 1
    assert isinstance(result.did_converge, bool)
    assert "p50" in result.centrality_distribution


def test_eigenvector_stream(eigenvector_endpoints: EigenvectorArrowEndpoints, sample_graph: Graph) -> None:
    """Test Eigenvector stream operation."""
    result_df = eigenvector_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "score" in result_df.columns
    assert len(result_df.columns) == 2


def test_eigenvector_mutate(eigenvector_endpoints: EigenvectorArrowEndpoints, sample_graph: Graph) -> None:
    """Test Eigenvector mutate operation."""
    result = eigenvector_endpoints.mutate(
        G=sample_graph,
        mutate_property="eigenvector",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 4
    assert result.ran_iterations >= 1
    assert isinstance(result.did_converge, bool)
    assert "p50" in result.centrality_distribution


def test_eigenvector_estimate(eigenvector_endpoints: EigenvectorArrowEndpoints, sample_graph: Graph) -> None:
    result = eigenvector_endpoints.estimate(sample_graph)

    assert result.node_count == 4
    assert result.relationship_count == 4
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
