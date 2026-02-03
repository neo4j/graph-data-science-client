from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.similarity.node_similarity_cypher_endpoints import (
    NodeSimilarityCypherEndpoints,
)
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node),
    (b: Node),
    (c: Node),
    (d: Node),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d),
    (d)-[:REL]->(a),
    (a)-[:REL]->(c),
    (b)-[:REL]->(d)
    """

    projection_query = """
        MATCH (n)--(m)
        WITH gds.graph.project('g', n, m) AS G
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
def node_similarity_endpoints(query_runner: QueryRunner) -> Generator[NodeSimilarityCypherEndpoints, None, None]:
    yield NodeSimilarityCypherEndpoints(query_runner)


def test_node_similarity_stats(node_similarity_endpoints: NodeSimilarityCypherEndpoints, sample_graph: GraphV2) -> None:
    result = node_similarity_endpoints.stats(G=sample_graph, top_k=2)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.nodes_compared > 0
    assert result.similarity_pairs > 0
    assert "p50" in result.similarity_distribution


def test_node_similarity_stream(
    node_similarity_endpoints: NodeSimilarityCypherEndpoints, sample_graph: GraphV2
) -> None:
    result_df = node_similarity_endpoints.stream(
        G=sample_graph,
        top_k=2,
    )

    assert set(result_df.columns) == {"node1", "node2", "similarity"}
    assert len(result_df) > 0


def test_node_similarity_mutate(
    node_similarity_endpoints: NodeSimilarityCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = node_similarity_endpoints.mutate(
        G=sample_graph,
        mutate_relationship_type="SIMILAR",
        mutate_property="similarity",
        top_k=2,
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.relationships_written > 0
    assert result.nodes_compared > 0


def test_node_similarity_write(node_similarity_endpoints: NodeSimilarityCypherEndpoints, sample_graph: GraphV2) -> None:
    result = node_similarity_endpoints.write(
        G=sample_graph,
        write_relationship_type="SIMILAR",
        write_property="similarity",
        top_k=2,
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.relationships_written > 0
    assert result.nodes_compared > 0


def test_node_similarity_estimate(
    node_similarity_endpoints: NodeSimilarityCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = node_similarity_endpoints.estimate(sample_graph, top_k=2)

    assert result.node_count == 4
    assert result.relationship_count == 12
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
