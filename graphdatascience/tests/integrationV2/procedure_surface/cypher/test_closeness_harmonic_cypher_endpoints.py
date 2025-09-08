from typing import Generator

import pytest

from graphdatascience import Graph, QueryRunner
from graphdatascience.procedure_surface.cypher.closeness_harmonic_cypher_endpoints import (
    ClosenessHarmonicCypherEndpoints,
)
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_statement = """
    CREATE
    (a: Node {name: 'a'}),
    (b: Node {name: 'b'}),
    (c: Node {name: 'c'}),
    (d: Node {name: 'd'}),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {}) AS G
        RETURN G
    """

    with create_graph(
            query_runner,
            'g',
            create_statement,
            projection_query,
    ) as g:
        yield g


@pytest.fixture
def closeness_harmonic_endpoints(query_runner: QueryRunner) -> Generator[ClosenessHarmonicCypherEndpoints, None, None]:
    yield ClosenessHarmonicCypherEndpoints(query_runner)


def test_closeness_harmonic_stats(
    closeness_harmonic_endpoints: ClosenessHarmonicCypherEndpoints, sample_graph: Graph
) -> None:
    """Test Harmonic Closeness stats operation."""
    result = closeness_harmonic_endpoints.stats(G=sample_graph)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert "p50" in result.centrality_distribution


def test_closeness_harmonic_stream(
    closeness_harmonic_endpoints: ClosenessHarmonicCypherEndpoints, sample_graph: Graph
) -> None:
    """Test Harmonic Closeness stream operation."""
    result_df = closeness_harmonic_endpoints.stream(
        G=sample_graph,
    )

    assert set(result_df.columns) == {"nodeId", "score"}
    assert len(result_df) == 4


def test_closeness_harmonic_mutate(
    closeness_harmonic_endpoints: ClosenessHarmonicCypherEndpoints, sample_graph: Graph
) -> None:
    """Test Harmonic Closeness mutate operation."""
    result = closeness_harmonic_endpoints.mutate(
        G=sample_graph,
        mutate_property="harmonic_closeness",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 4
    assert "p50" in result.centrality_distribution


def test_closeness_harmonic_estimate(
    closeness_harmonic_endpoints: ClosenessHarmonicCypherEndpoints, sample_graph: Graph
) -> None:
    result = closeness_harmonic_endpoints.estimate(sample_graph)

    assert result.node_count == 4
    assert result.relationship_count == 3
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
