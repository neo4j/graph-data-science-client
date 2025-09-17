from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import Graph
from graphdatascience.procedure_surface.cypher.hashgnn_cypher_endpoints import HashGNNCypherEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_statement = """
    CREATE
    (a: Node {feature: [1, 0, 1, 0]}),
    (b: Node {feature: [0, 1, 0, 1]}),
    (c: Node {feature: [1, 1, 0, 0]}),
    (d: Node {feature: [0, 0, 1, 1]}),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d),
    (d)-[:REL]->(a)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {sourceNodeProperties: properties(n), targetNodeProperties: properties(m)}) AS G
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
def hashgnn_endpoints(query_runner: QueryRunner) -> Generator[HashGNNCypherEndpoints, None, None]:
    yield HashGNNCypherEndpoints(query_runner)


def test_hashgnn_mutate(hashgnn_endpoints: HashGNNCypherEndpoints, sample_graph: Graph) -> None:
    """Test HashGNN mutate operation."""
    result = hashgnn_endpoints.mutate(
        G=sample_graph,
        iterations=2,
        embedding_density=256,
        mutate_property="hashgnn_embedding",
        feature_properties=["feature"],
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 4
    assert result.configuration is not None


def test_hashgnn_stream(hashgnn_endpoints: HashGNNCypherEndpoints, sample_graph: Graph) -> None:
    """Test HashGNN stream operation."""
    result = hashgnn_endpoints.stream(
        G=sample_graph,
        iterations=2,
        embedding_density=128,
        feature_properties=["feature"],
    )

    assert len(result) == 4  # We have 4 nodes
    assert set(result.columns) == {"nodeId", "embedding"}


def test_hashgnn_write(hashgnn_endpoints: HashGNNCypherEndpoints, sample_graph: Graph) -> None:
    """Test HashGNN write operation."""
    result = hashgnn_endpoints.write(
        G=sample_graph,
        iterations=2,
        embedding_density=64,
        write_property="hashgnn_embedding",
        feature_properties=["feature"],
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 4
    assert result.configuration is not None


def test_hashgnn_estimate(hashgnn_endpoints: HashGNNCypherEndpoints, sample_graph: Graph) -> None:
    """Test HashGNN estimate operation."""
    result = hashgnn_endpoints.estimate(
        G=sample_graph,
        iterations=2,
        embedding_density=2,
        feature_properties=["feature"],
    )

    assert result.node_count == 4
    assert result.relationship_count == 4
    assert "KiB" in result.required_memory or "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
