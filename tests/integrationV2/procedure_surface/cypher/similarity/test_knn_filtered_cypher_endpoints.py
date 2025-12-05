from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.similarity.knn_filtered_cypher_endpoints import (
    KnnFilteredCypherEndpoints,
)
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: SourceNode {prop: [1.0, 2.0, 3.0]}),
    (b: SourceNode {prop: [2.0, 3.0, 4.0]}),
    (c: TargetNode {prop: [3.0, 4.0, 5.0]}),
    (d: TargetNode {prop: [1.0, 1.0, 1.0]})
    """

    projection_query = """
        MATCH (n)
        WITH gds.graph.project('g', n, null, {sourceNodeProperties: properties(n), sourceNodeLabels: labels(n), targetNodeProperties: null, targetNodeLabels: null}) AS G
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
def knn_filtered_endpoints(query_runner: QueryRunner) -> Generator[KnnFilteredCypherEndpoints, None, None]:
    yield KnnFilteredCypherEndpoints(query_runner)


def test_knn_filtered_stats(knn_filtered_endpoints: KnnFilteredCypherEndpoints, sample_graph: GraphV2) -> None:
    result = knn_filtered_endpoints.stats(
        G=sample_graph,
        node_properties=["prop"],
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert result.ran_iterations > 0
    assert result.did_converge
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.nodes_compared > 0
    assert result.similarity_pairs > 0
    assert "p50" in result.similarity_distribution
    assert result.node_pairs_considered > 0
    assert "concurrency" in result.configuration


def test_knn_filtered_stream(knn_filtered_endpoints: KnnFilteredCypherEndpoints, sample_graph: GraphV2) -> None:
    result = knn_filtered_endpoints.stream(
        G=sample_graph,
        node_properties=["prop"],
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert set(result.columns) == {"node1", "node2", "similarity"}
    assert len(result) >= 4


def test_knn_filtered_mutate(knn_filtered_endpoints: KnnFilteredCypherEndpoints, sample_graph: GraphV2) -> None:
    result = knn_filtered_endpoints.mutate(
        G=sample_graph,
        node_properties=["prop"],
        mutate_property="score",
        mutate_relationship_type="SIMILAR_TO",
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert result.ran_iterations > 0
    assert result.did_converge
    assert result.compute_millis > 0
    assert result.mutate_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.nodes_compared > 0
    assert result.relationships_written > 0
    assert "p50" in result.similarity_distribution
    assert result.node_pairs_considered > 0
    assert "concurrency" in result.configuration


def test_knn_filtered_write(knn_filtered_endpoints: KnnFilteredCypherEndpoints, sample_graph: GraphV2) -> None:
    result = knn_filtered_endpoints.write(
        G=sample_graph,
        node_properties=["prop"],
        write_property="score",
        write_relationship_type="SIMILAR_TO",
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert result.ran_iterations > 0
    assert result.did_converge
    assert result.compute_millis > 0
    assert result.write_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.nodes_compared > 0
    assert result.relationships_written > 0
    assert "p50" in result.similarity_distribution
    assert result.node_pairs_considered > 0
    assert "concurrency" in result.configuration


def test_knn_filtered_estimate(knn_filtered_endpoints: KnnFilteredCypherEndpoints, sample_graph: GraphV2) -> None:
    result = knn_filtered_endpoints.estimate(
        G=sample_graph,
        node_properties=["prop"],
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert result.required_memory is not None
    assert result.tree_view is not None
    assert result.map_view is not None
