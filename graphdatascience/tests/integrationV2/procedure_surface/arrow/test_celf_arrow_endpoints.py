from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.catalog.graph_api import Graph
from graphdatascience.procedure_surface.arrow.celf_arrow_endpoints import CelfArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    (a: Node {id: 0})
    (b: Node {id: 1})
    (c: Node {id: 2})
    (d: Node {id: 3})
    (e: Node {id: 4})
    (a)-[:REL]->(b)
    (a)-[:REL]->(c)
    (b)-[:REL]->(d)
    (c)-[:REL]->(e)
    (d)-[:REL]->(e)
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def celf_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[CelfArrowEndpoints, None, None]:
    yield CelfArrowEndpoints(arrow_client)


def test_celf_stats(celf_endpoints: CelfArrowEndpoints, sample_graph: Graph) -> None:
    """Test CELF stats operation."""
    result = celf_endpoints.stats(G=sample_graph, seed_set_size=2)

    assert result.compute_millis >= 0
    assert result.total_spread >= 0.0
    assert result.node_count == 5
    assert isinstance(result.configuration, dict)


def test_celf_stream(celf_endpoints: CelfArrowEndpoints, sample_graph: Graph) -> None:
    """Test CELF stream operation."""
    result_df = celf_endpoints.stream(G=sample_graph, seed_set_size=2)

    assert set(result_df.columns) == {"nodeId", "spread"}
    assert len(result_df) == 5  # same as node count
    assert all(result_df["spread"] >= 0)


def test_celf_mutate(celf_endpoints: CelfArrowEndpoints, sample_graph: Graph) -> None:
    """Test CELF mutate operation."""
    result = celf_endpoints.mutate(G=sample_graph, seed_set_size=2, mutate_property="celf_spread")

    assert result.node_properties_written == 5  # All nodes get properties (influence spread values)
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.total_spread >= 0.0
    assert result.node_count == 5
    assert isinstance(result.configuration, dict)


def test_celf_write_without_write_back_client(celf_endpoints: CelfArrowEndpoints, sample_graph: Graph) -> None:
    """Test CELF write operation raises exception when write_back_client is None."""
    with pytest.raises(Exception, match="Write back client is not initialized"):
        celf_endpoints.write(
            G=sample_graph,
            seed_set_size=2,
            write_property="celf_spread",
        )


def test_celf_estimate(celf_endpoints: CelfArrowEndpoints, sample_graph: Graph) -> None:
    """Test CELF memory estimation."""
    result = celf_endpoints.estimate(G=sample_graph, seed_set_size=2)

    assert result.node_count == 5
    assert result.relationship_count >= 0
    assert "Bytes" in result.required_memory or "KiB" in result.required_memory or "MiB" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
