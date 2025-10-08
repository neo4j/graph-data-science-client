from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.sllpa_cypher_endpoints import SllpaCypherEndpoints
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
def sllpa_endpoints(query_runner: QueryRunner) -> Generator[SllpaCypherEndpoints, None, None]:
    yield SllpaCypherEndpoints(query_runner)


def test_sllpa_stats(sllpa_endpoints: SllpaCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test SLLPA stats operation."""
    result = sllpa_endpoints.stats(G=sample_graph, max_iterations=1)

    assert result.ran_iterations > 0
    assert result.did_converge in [True, False]
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert isinstance(result.configuration, dict)


def test_sllpa_stream(sllpa_endpoints: SllpaCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test SLLPA stream operation."""
    result_df = sllpa_endpoints.stream(G=sample_graph, max_iterations=1)

    assert len(result_df) == 6  # 6 nodes in the graph
    assert "nodeId" in result_df.columns
    assert "values" in result_df.columns


def test_sllpa_mutate(sllpa_endpoints: SllpaCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test SLLPA mutate operation."""
    result = sllpa_endpoints.mutate(
        G=sample_graph,
        mutate_property="sllpa_community",
        max_iterations=1,
    )

    assert result.ran_iterations > 0
    assert result.did_converge in [True, False]
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 6
    assert isinstance(result.configuration, dict)


def test_sllpa_write(sllpa_endpoints: SllpaCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test SLLPA write operation."""
    result = sllpa_endpoints.write(
        G=sample_graph,
        write_property="sllpa_community_write",
        max_iterations=1,
    )

    assert result.ran_iterations > 0
    assert result.did_converge in [True, False]
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 6
    assert isinstance(result.configuration, dict)


def test_sllpa_estimate(sllpa_endpoints: SllpaCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test SLLPA estimate operation."""
    result = sllpa_endpoints.estimate(sample_graph, max_iterations=1)

    assert result.node_count == 6
    assert result.relationship_count == 7
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
