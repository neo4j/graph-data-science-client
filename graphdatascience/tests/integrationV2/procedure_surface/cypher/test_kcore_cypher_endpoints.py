from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.kcore_cypher_endpoints import KCoreCypherEndpoints
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
    (b)-[:REL]->(c),
    (c)-[:REL]->(a),
    (d)-[:REL]->(e),
    (e)-[:REL]->(f),
    (f)-[:REL]->(d),
    (a)-[:REL]->(d)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('kcore_g', n, m, {relationshipType: "REL"}, {undirectedRelationshipTypes: ["REL"]}) AS G
        RETURN G
    """

    with create_graph(
        query_runner,
        "kcore_g",
        create_statement,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def kcore_endpoints(query_runner: QueryRunner) -> Generator[KCoreCypherEndpoints, None, None]:
    yield KCoreCypherEndpoints(query_runner)


def test_kcore_stats(kcore_endpoints: KCoreCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test K-Core stats operation."""
    result = kcore_endpoints.stats(G=sample_graph)

    assert result.degeneracy >= 1
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0


def test_kcore_stream(kcore_endpoints: KCoreCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test K-Core stream operation."""
    result_df = kcore_endpoints.stream(G=sample_graph)

    assert "nodeId" in result_df.columns
    assert "coreValue" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 6


def test_kcore_mutate(kcore_endpoints: KCoreCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test K-Core mutate operation."""
    result = kcore_endpoints.mutate(G=sample_graph, mutate_property="coreValue")

    assert result.degeneracy >= 1
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 6


def test_kcore_write(kcore_endpoints: KCoreCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test K-Core write operation."""
    result = kcore_endpoints.write(
        G=sample_graph,
        write_property="coreValue",
    )

    assert result.degeneracy >= 1
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 6


def test_kcore_estimate(kcore_endpoints: KCoreCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test K-Core estimate operation."""
    result = kcore_endpoints.estimate(sample_graph)

    assert result.node_count == 6
    assert result.relationship_count == 14
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0


def test_kcore_stats_with_parameters(kcore_endpoints: KCoreCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test K-Core stats operation with various parameters."""
    result = kcore_endpoints.stats(G=sample_graph, relationship_types=["REL"], concurrency=2)

    assert result.degeneracy >= 1
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0


def test_kcore_stream_with_parameters(kcore_endpoints: KCoreCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test K-Core stream operation with various parameters."""
    result_df = kcore_endpoints.stream(G=sample_graph, relationship_types=["REL"], concurrency=2)

    assert "nodeId" in result_df.columns
    assert "coreValue" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 6


def test_kcore_mutate_with_parameters(kcore_endpoints: KCoreCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test K-Core mutate operation with various parameters."""
    result = kcore_endpoints.mutate(
        G=sample_graph, mutate_property="kcoreValue", relationship_types=["REL"], concurrency=2
    )

    assert result.degeneracy >= 1
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 6


def test_kcore_write_with_parameters(kcore_endpoints: KCoreCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test K-Core write operation with various parameters."""
    result = kcore_endpoints.write(
        G=sample_graph,
        write_property="kcoreValue",
        relationship_types=["REL"],
        concurrency=2,
        write_concurrency=1,
    )

    assert result.degeneracy >= 1
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 6
