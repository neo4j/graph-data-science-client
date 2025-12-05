from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.similarity.node_similarity_filtered_cypher_endpoints import (
    NodeSimilarityFilteredCypherEndpoints,
)
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: SourceNode),
    (b: SourceNode),
    (c: TargetNode),
    (d: TargetNode),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d),
    (d)-[:REL]->(a),
    (a)-[:REL]->(c),
    (b)-[:REL]->(d)
    """

    projection_query = """
        MATCH (n)--(m)
        WITH gds.graph.project('g', n, m, {sourceNodeLabels: labels(n), targetNodeLabels: labels(m)}) AS G
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
def node_similarity_filtered_endpoints(
    query_runner: QueryRunner,
) -> Generator[NodeSimilarityFilteredCypherEndpoints, None, None]:
    yield NodeSimilarityFilteredCypherEndpoints(query_runner)


def test_node_similarity_filtered_stats(
    node_similarity_filtered_endpoints: NodeSimilarityFilteredCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = node_similarity_filtered_endpoints.stats(
        G=sample_graph,
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.nodes_compared > 0
    assert result.similarity_pairs > 0
    assert "p50" in result.similarity_distribution
    assert "concurrency" in result.configuration


def test_node_similarity_filtered_stream(
    node_similarity_filtered_endpoints: NodeSimilarityFilteredCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = node_similarity_filtered_endpoints.stream(
        G=sample_graph,
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert set(result.columns) == {"node1", "node2", "similarity"}
    assert len(result) >= 1


def test_node_similarity_filtered_mutate(
    node_similarity_filtered_endpoints: NodeSimilarityFilteredCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = node_similarity_filtered_endpoints.mutate(
        G=sample_graph,
        mutate_relationship_type="SIMILAR",
        mutate_property="similarity",
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.relationships_written > 0
    assert result.nodes_compared > 0
    assert "concurrency" in result.configuration


def test_node_similarity_filtered_write(
    node_similarity_filtered_endpoints: NodeSimilarityFilteredCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = node_similarity_filtered_endpoints.write(
        G=sample_graph,
        write_relationship_type="SIMILAR",
        write_property="similarity",
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.relationships_written > 0
    assert result.nodes_compared > 0
    assert "concurrency" in result.configuration


def test_node_similarity_filtered_estimate(
    node_similarity_filtered_endpoints: NodeSimilarityFilteredCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = node_similarity_filtered_endpoints.estimate(
        sample_graph,
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert result.node_count == 4
    assert result.relationship_count == 12
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
