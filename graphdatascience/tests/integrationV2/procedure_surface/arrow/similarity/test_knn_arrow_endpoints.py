from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.similarity.knn_endpoints import KnnWriteResult
from graphdatascience.procedure_surface.arrow.similarity.knn_arrow_endpoints import KnnArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
        CREATE
            (a: Node {prop: [1.0, 2.0, 3.0]}),
            (b: Node {prop: [2.0, 2.0, 4.0]}),
            (c: Node {prop: [3.0, 2.0, 1.0]}),
            (d: Node {prop: [4.0, 2.0, 0.0]})
        """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "g", graph) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        graph,
        """
                    MATCH (n)
                    WITH gds.graph.project.remote(n, null, {sourceNodeProperties: properties(n)}) as g
                    RETURN g
                """,
    ) as g:
        yield g


@pytest.fixture
def knn_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[KnnArrowEndpoints, None, None]:
    yield KnnArrowEndpoints(arrow_client)


def test_knn_stats(knn_endpoints: KnnArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test KNN stats operation."""
    result = knn_endpoints.stats(G=sample_graph, node_properties=["prop"], top_k=2)

    assert result.ran_iterations >= 0
    assert result.did_converge in [True, False]
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.nodes_compared > 0
    assert result.similarity_pairs >= 0
    assert result.node_pairs_considered > 0
    assert "p50" in result.similarity_distribution


@pytest.mark.skip(reason="SEGFAULT for custom metadata. tracked in GDSA-312")
def test_knn_stream(knn_endpoints: KnnArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test KNN stream operation."""
    result_df = knn_endpoints.stream(
        G=sample_graph,
        node_properties=["prop"],
        top_k=2,
    )

    assert set(result_df.columns) == {"node1", "node2", "similarity"}
    assert len(result_df) == 2


def test_knn_mutate(knn_endpoints: KnnArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test KNN mutate operation."""
    result = knn_endpoints.mutate(
        G=sample_graph,
        mutate_relationship_type="SIMILAR",
        mutate_property="similarity",
        node_properties=["prop"],
        top_k=2,
    )

    assert result.ran_iterations >= 0
    assert result.did_converge in [True, False]
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.relationships_written == sample_graph.node_count() * 2
    assert result.node_pairs_considered >= 0


@pytest.mark.db_integration
def test_knn_write(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2) -> None:
    """Test KNN write operation."""
    endpoints = KnnArrowEndpoints(
        arrow_client, write_back_client=RemoteWriteBackClient(arrow_client, query_runner), show_progress=False
    )

    result = endpoints.write(
        G=db_graph, write_relationship_type="SIMILAR", write_property="similarity", node_properties=["prop"], top_k=2
    )

    assert isinstance(result, KnnWriteResult)
    assert result.ran_iterations >= 0
    assert result.did_converge in [True, False]
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.relationships_written == db_graph.node_count() * 2
    assert result.node_pairs_considered >= 0

    # Check that relationships were written to the database
    count_result = query_runner.run_cypher("MATCH ()-[r:SIMILAR]->() RETURN COUNT(r) AS count")
    assert count_result.squeeze() >= result.relationships_written


def test_knn_estimate(knn_endpoints: KnnArrowEndpoints, sample_graph: GraphV2) -> None:
    result = knn_endpoints.estimate(sample_graph, node_properties=["prop"], top_k=2)

    assert result.node_count == 4
    assert result.relationship_count == 0  # No relationships in this graph
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
