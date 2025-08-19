from typing import Generator

import pytest

from graphdatascience import Graph, QueryRunner
from graphdatascience.procedure_surface.cypher.fastrp_cypher_endpoints import FastRPCypherEndpoints


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_statement = """
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

    query_runner.run_cypher(create_statement)

    query_runner.run_cypher("""
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {}) AS G
        RETURN G
    """)

    yield Graph("g", query_runner)

    query_runner.run_cypher("CALL gds.graph.drop('g')")
    query_runner.run_cypher("MATCH (n) DETACH DELETE n")


@pytest.fixture
def fastrp_endpoints(query_runner: QueryRunner) -> Generator[FastRPCypherEndpoints, None, None]:
    yield FastRPCypherEndpoints(query_runner)


def test_fastrp_stats(fastrp_endpoints: FastRPCypherEndpoints, sample_graph: Graph) -> None:
    """Test FastRP stats operation."""
    result = fastrp_endpoints.stats(G=sample_graph, embedding_dimension=128)

    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.configuration is not None


def test_fastrp_stream(fastrp_endpoints: FastRPCypherEndpoints, sample_graph: Graph) -> None:
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


def test_fastrp_mutate(fastrp_endpoints: FastRPCypherEndpoints, sample_graph: Graph) -> None:
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


def test_fastrp_write(fastrp_endpoints: FastRPCypherEndpoints, sample_graph: Graph) -> None:
    """Test FastRP write operation."""
    result = fastrp_endpoints.write(
        G=sample_graph,
        write_property="fastrp_embedding",
        embedding_dimension=16,
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 4
    assert result.configuration is not None


def test_fastrp_estimate(fastrp_endpoints: FastRPCypherEndpoints, sample_graph: Graph) -> None:
    """Test FastRP estimate operation."""
    result = fastrp_endpoints.estimate(sample_graph, embedding_dimension=128)

    assert result.node_count == 4
    assert result.relationship_count == 4
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
