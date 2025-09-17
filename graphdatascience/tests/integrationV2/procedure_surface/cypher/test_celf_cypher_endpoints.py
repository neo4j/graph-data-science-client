from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.celf_cypher_endpoints import CelfCypherEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node {id: 0}),
    (b: Node {id: 1}),
    (c: Node {id: 2}),
    (d: Node {id: 3}),
    (e: Node {id: 4}),
    (a)-[:REL]->(b),
    (a)-[:REL]->(c),
    (b)-[:REL]->(d),
    (c)-[:REL]->(e),
    (d)-[:REL]->(e)
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
def celf_endpoints(query_runner: QueryRunner) -> Generator[CelfCypherEndpoints, None, None]:
    yield CelfCypherEndpoints(query_runner)


def test_celf_stats(celf_endpoints: CelfCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test CELF stats operation."""
    result = celf_endpoints.stats(G=sample_graph, seed_set_size=2)

    assert result.compute_millis >= 0
    assert result.total_spread >= 0.0
    assert result.node_count == 5
    assert isinstance(result.configuration, dict)
    assert result.configuration.get("seedSetSize") == 2


def test_celf_stream(celf_endpoints: CelfCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test CELF stream operation."""
    result_df = celf_endpoints.stream(G=sample_graph, seed_set_size=2)

    assert "nodeId" in result_df.columns
    assert "spread" in result_df.columns
    assert len(result_df) == 2  # We requested 2 nodes in seed set
    assert all(result_df["spread"] >= 0)  # Spread values should be non-negative


def test_celf_mutate(celf_endpoints: CelfCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test CELF mutate operation."""
    result = celf_endpoints.mutate(G=sample_graph, seed_set_size=2, mutate_property="celf_spread")

    assert result.node_properties_written == 5  # All nodes get properties (influence spread values)
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.total_spread >= 0.0
    assert result.node_count == 5
    assert isinstance(result.configuration, dict)
    assert result.configuration.get("mutateProperty") == "celf_spread"
    assert result.configuration.get("seedSetSize") == 2


def test_celf_write(celf_endpoints: CelfCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test CELF write operation."""
    result = celf_endpoints.write(G=sample_graph, seed_set_size=2, write_property="celf_spread")

    assert result.node_properties_written == 5  # All nodes get properties (influence spread values)
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.total_spread >= 0.0
    assert result.node_count == 5
    assert isinstance(result.configuration, dict)
    assert result.configuration.get("writeProperty") == "celf_spread"
    assert result.configuration.get("seedSetSize") == 2


def test_celf_estimate(celf_endpoints: CelfCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test CELF memory estimation."""
    result = celf_endpoints.estimate(G=sample_graph, seed_set_size=2)

    assert result.node_count == 5
    assert result.relationship_count == 5
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
