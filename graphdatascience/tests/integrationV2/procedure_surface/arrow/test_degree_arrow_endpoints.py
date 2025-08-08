import json
from typing import Generator

import pytest

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.degree_arrow_endpoints import DegreeArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    (a: Node {id: 0})
    (b: Node {id: 1})
    (c: Node {id: 2})
    (a)-[:REL]->(c)
    (b)-[:REL]->(c)
    """

    yield create_graph(arrow_client, "g", gdl)
    arrow_client.do_action("v2/graph.drop", json.dumps({"graphName": "g"}).encode("utf-8"))


@pytest.fixture
def degree_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[DegreeArrowEndpoints, None, None]:
    yield DegreeArrowEndpoints(arrow_client)


def test_degree_stats(degree_endpoints: DegreeArrowEndpoints, sample_graph: Graph) -> None:
    """Test Degree stats operation."""
    result = degree_endpoints.stats(G=sample_graph)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert "p50" in result.centrality_distribution
    assert isinstance(result.configuration, dict)


def test_degree_stream(degree_endpoints: DegreeArrowEndpoints, sample_graph: Graph) -> None:
    """Test Degree stream operation."""
    result_df = degree_endpoints.stream(G=sample_graph)

    assert "nodeId" in result_df.columns
    assert "score" in result_df.columns
    assert len(result_df) == 3  # We have 3 nodes
    assert all(result_df["score"] >= 0)  # Degree scores should be non-negative


def test_degree_mutate(degree_endpoints: DegreeArrowEndpoints, sample_graph: Graph) -> None:
    """Test Degree mutate operation."""
    result = degree_endpoints.mutate(G=sample_graph, mutate_property="degree")

    assert result.node_properties_written == 3
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert "p50" in result.centrality_distribution
    assert isinstance(result.configuration, dict)


def test_degree_estimate(degree_endpoints: DegreeArrowEndpoints, sample_graph: Graph) -> None:
    result = degree_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 2
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
