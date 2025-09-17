from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.degree_cypher_endpoints import DegreeCypherEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node {id: 0}),
    (b: Node {id: 1}),
    (c: Node {id: 2}),
    (a)-[:REL]->(c),
    (b)-[:REL]->(c)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {}) AS G
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
def degree_endpoints(query_runner: QueryRunner) -> Generator[DegreeCypherEndpoints, None, None]:
    yield DegreeCypherEndpoints(query_runner)


def test_degree_stats(degree_endpoints: DegreeCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Degree stats operation."""
    result = degree_endpoints.stats(G=sample_graph)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert "p50" in result.centrality_distribution
    assert isinstance(result.configuration, dict)


def test_degree_stream(degree_endpoints: DegreeCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Degree stream operation."""
    result = degree_endpoints.stream(G=sample_graph)

    assert len(result) == 3  # We have 3 nodes
    assert "nodeId" in result.columns
    assert "score" in result.columns


def test_degree_mutate(degree_endpoints: DegreeCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Degree mutate operation."""
    result = degree_endpoints.mutate(G=sample_graph, mutate_property="degree")

    assert result.node_properties_written == 3
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert "p50" in result.centrality_distribution
    assert isinstance(result.configuration, dict)
    assert result.configuration.get("mutateProperty") == "degree"


def test_degree_write(degree_endpoints: DegreeCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Degree write operation."""
    result = degree_endpoints.write(G=sample_graph, write_property="degree")

    assert result.node_properties_written == 3
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert "p50" in result.centrality_distribution
    assert isinstance(result.configuration, dict)
    assert result.configuration.get("writeProperty") == "degree"


def test_degree_estimate(degree_endpoints: DegreeCypherEndpoints, sample_graph: GraphV2) -> None:
    result = degree_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 2
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
