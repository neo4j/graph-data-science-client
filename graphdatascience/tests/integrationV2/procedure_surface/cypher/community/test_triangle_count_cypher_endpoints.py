from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.community.triangle_count_cypher_endpoints import (
    TriangleCountCypherEndpoints,
)
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node),
    (b: Node),
    (c: Node),
    (d: Node),
    (e: Node),
    (f: Node),
    (a)-[:REL]->(b),
    (a)-[:REL]->(c),
    (b)-[:REL]->(c),
    (d)-[:REL]->(e),
    (d)-[:REL]->(f),
    (e)-[:REL]->(f),
    (a)-[:REL]->(d)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {relationshipType: "REL"}, {undirectedRelationshipTypes: ["REL"]}) AS G
        RETURN G
    """

    with create_graph(
        query_runner,
        "g",
        create_statement,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def triangle_count_endpoints(query_runner: QueryRunner) -> Generator[TriangleCountCypherEndpoints, None, None]:
    yield TriangleCountCypherEndpoints(query_runner)


def test_triangle_count_stats(triangle_count_endpoints: TriangleCountCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Triangle Count stats operation."""
    result = triangle_count_endpoints.stats(G=sample_graph)

    assert result.global_triangle_count >= 0  # Should have at least 0 triangles
    assert result.node_count == 6  # We have 6 nodes
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0


def test_triangle_count_stream(triangle_count_endpoints: TriangleCountCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Triangle Count stream operation."""
    result_df = triangle_count_endpoints.stream(G=sample_graph)

    assert "nodeId" in result_df.columns
    assert "triangleCount" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 6  # We have 6 nodes

    # Check that triangle counts are non-negative
    assert all(result_df["triangleCount"] >= 0)


def test_triangle_count_mutate(triangle_count_endpoints: TriangleCountCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Triangle Count mutate operation."""
    result = triangle_count_endpoints.mutate(
        G=sample_graph,
        mutate_property="triangle_count",
    )

    assert result.global_triangle_count >= 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 6  # All 6 nodes should have triangle count assigned
    assert result.node_count == 6


def test_triangle_count_write(triangle_count_endpoints: TriangleCountCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Triangle Count write operation."""
    result = triangle_count_endpoints.write(
        G=sample_graph,
        write_property="triangle_count_write",
    )

    assert result.global_triangle_count >= 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 6  # All 6 nodes should have triangle count written
    assert result.node_count == 6


def test_triangle_count_estimate(triangle_count_endpoints: TriangleCountCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Triangle Count estimate operation."""
    result = triangle_count_endpoints.estimate(sample_graph)

    assert result.node_count == 6
    assert result.relationship_count == 14
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
