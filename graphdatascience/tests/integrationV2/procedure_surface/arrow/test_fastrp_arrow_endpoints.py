from typing import Generator

import pytest

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.fastrp_arrow_endpoints import FastRPArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    CREATE
    (a: Node),
    (b: Node),
    (c: Node),
    (d: Node),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d),
    (d)-[:REL]->(a)
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def fastrp_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[FastRPArrowEndpoints, None, None]:
    yield FastRPArrowEndpoints(arrow_client)


def test_fastrp_stats(fastrp_endpoints: FastRPArrowEndpoints, sample_graph: Graph) -> None:
    """Test FastRP stats operation."""
    result = fastrp_endpoints.stats(G=sample_graph, embedding_dimension=128)

    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.configuration is not None


def test_fastrp_stream(fastrp_endpoints: FastRPArrowEndpoints, sample_graph: Graph) -> None:
    """Test FastRP stream operation."""
    result_df = fastrp_endpoints.stream(
        G=sample_graph,
        embedding_dimension=64,
    )

    assert set(result_df.columns) == {"nodeId", "embedding"}
    assert len(result_df.columns) == 2
    assert len(result_df) == 4  # We have 4 nodes

    # Check that embeddings have the correct dimension
    embedding_sample = result_df["embedding"].iloc[0]
    assert len(embedding_sample) == 64


def test_fastrp_mutate(fastrp_endpoints: FastRPArrowEndpoints, sample_graph: Graph) -> None:
    """Test FastRP mutate operation."""
    result = fastrp_endpoints.mutate(
        G=sample_graph,
        mutate_property="fastrp_embedding",
        embedding_dimension=32,
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 4
    assert result.configuration is not None


def test_fastrp_write(fastrp_endpoints: FastRPArrowEndpoints, sample_graph: Graph) -> None:
    with pytest.raises(Exception, match="Write back client is not initialized"):
        fastrp_endpoints.write(
            G=sample_graph,
            write_property="fastrp_embedding",
            embedding_dimension=2,
        )


def test_fastrp_estimate(fastrp_endpoints: FastRPArrowEndpoints, sample_graph: Graph) -> None:
    """Test FastRP estimate operation."""
    result = fastrp_endpoints.estimate(sample_graph, embedding_dimension=128)

    assert result.node_count == 4
    assert result.relationship_count == 4
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
