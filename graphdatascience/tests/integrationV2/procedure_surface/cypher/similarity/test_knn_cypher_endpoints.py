from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.similarity.knn_cypher_endpoints import KnnCypherEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node {prop: [1.0, 2.0, 3.0]}),
    (b: Node {prop: [2.0, 3.0, 4.0]}),
    (c: Node {prop: [3.0, 4.0, 5.0]}),
    (d: Node {prop: [1.0, 1.0, 1.0]})
    """

    projection_query = """
        MATCH (n)
        WITH gds.graph.project('g', n, null, {sourceNodeProperties: properties(n), targetNodeProperties: null}) AS G
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
def knn_endpoints(query_runner: QueryRunner) -> Generator[KnnCypherEndpoints, None, None]:
    yield KnnCypherEndpoints(query_runner)


def test_knn_stats(knn_endpoints: KnnCypherEndpoints, sample_graph: GraphV2) -> None:
    result = knn_endpoints.stats(G=sample_graph, node_properties=["prop"], top_k=2)

    assert result.ran_iterations > 0
    assert result.did_converge
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.nodes_compared > 0
    assert result.similarity_pairs == 8
    assert result.node_pairs_considered > 0
    assert "p50" in result.similarity_distribution


def test_knn_stream(knn_endpoints: KnnCypherEndpoints, sample_graph: GraphV2) -> None:
    result_df = knn_endpoints.stream(
        G=sample_graph,
        node_properties=["prop"],
        top_k=2,
    )

    assert set(result_df.columns) == {"node1", "node2", "similarity"}
    assert len(result_df) == 8


def test_knn_mutate(knn_endpoints: KnnCypherEndpoints, sample_graph: GraphV2) -> None:
    result = knn_endpoints.mutate(
        G=sample_graph,
        mutate_relationship_type="SIMILAR",
        mutate_property="similarity",
        node_properties=["prop"],
        top_k=2,
    )

    assert result.ran_iterations > 0
    assert result.did_converge
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.relationships_written == 8
    assert result.node_pairs_considered > 0


def test_knn_estimate(knn_endpoints: KnnCypherEndpoints, sample_graph: GraphV2) -> None:
    result = knn_endpoints.estimate(sample_graph, node_properties=["prop"], top_k=2)

    assert result.node_count == 4
    assert result.relationship_count == 0  # No relationships in this graph
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
