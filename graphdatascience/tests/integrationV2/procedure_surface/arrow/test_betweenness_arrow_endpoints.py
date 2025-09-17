from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.betweenness_endpoints import (
    BetweennessMutateResult,
    BetweennessStatsResult,
)
from graphdatascience.procedure_surface.api.catalog.graph_api import Graph
from graphdatascience.procedure_surface.arrow.betweenness_arrow_endpoints import BetweennessArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    (a: Node)
    (b: Node)
    (c: Node)
    (a)-[:REL]->(b)
    (b)-[:REL]->(c)
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def betweenness_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[BetweennessArrowEndpoints, None, None]:
    yield BetweennessArrowEndpoints(arrow_client)


def test_betweenness_stats(betweenness_endpoints: BetweennessArrowEndpoints, sample_graph: Graph) -> None:
    """Test Betweenness Centrality stats operation."""
    result = betweenness_endpoints.stats(
        G=sample_graph,
    )

    assert isinstance(result, BetweennessStatsResult)
    assert "p50" in result.centrality_distribution
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0


def test_betweenness_stream(betweenness_endpoints: BetweennessArrowEndpoints, sample_graph: Graph) -> None:
    """Test Betweenness Centrality stream operation."""
    result = betweenness_endpoints.stream(
        G=sample_graph,
    )

    assert len(result) == 3  # Should have 3 nodes
    assert {"nodeId", "score"} == set(result.columns.to_list())


def test_betweenness_mutate(betweenness_endpoints: BetweennessArrowEndpoints, sample_graph: Graph) -> None:
    """Test Betweenness Centrality mutate operation."""
    result = betweenness_endpoints.mutate(
        G=sample_graph,
        mutate_property="betweenness",
    )

    assert isinstance(result, BetweennessMutateResult)
    assert "p50" in result.centrality_distribution
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 3


def test_betweenness_write_without_write_back_client(
    betweenness_endpoints: BetweennessArrowEndpoints, sample_graph: Graph
) -> None:
    """Test Betweenness Centrality write operation raises exception when write_back_client is None."""
    with pytest.raises(Exception, match="Write back client is not initialized"):
        betweenness_endpoints.write(
            G=sample_graph,
            write_property="betweenness",
        )


def test_betweenness_estimate(betweenness_endpoints: BetweennessArrowEndpoints, sample_graph: Graph) -> None:
    result = betweenness_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 2
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
