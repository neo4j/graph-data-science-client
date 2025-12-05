from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.similarity.knn_filtered_arrow_endpoints import KnnFilteredArrowEndpoints
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
        CREATE
            (a: SourceNode {prop: [1.0, 2.0, 3.0]}),
            (b: SourceNode {prop: [2.0, 2.0, 4.0]}),
            (c: TargetNode {prop: [3.0, 2.0, 1.0]}),
            (d: TargetNode {prop: [4.0, 2.0, 0.0]})
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
                    WITH gds.graph.project.remote(n, null, {sourceNodeProperties: properties(n), sourceNodeLabels: labels(n)}) as g
                    RETURN g
                """,
    ) as g:
        yield g


@pytest.fixture
def knn_filtered_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[KnnFilteredArrowEndpoints, None, None]:
    yield KnnFilteredArrowEndpoints(arrow_client)


def test_knn_filtered_stats(knn_filtered_endpoints: KnnFilteredArrowEndpoints, sample_graph: GraphV2) -> None:
    result = knn_filtered_endpoints.stats(
        sample_graph,
        node_properties="prop",
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.nodes_compared >= 0
    assert result.similarity_pairs >= 0
    assert "p50" in result.similarity_distribution
    assert result.did_converge
    assert result.ran_iterations >= 0
    assert result.node_pairs_considered >= 0
    assert "concurrency" in result.configuration


def test_knn_filtered_stream(knn_filtered_endpoints: KnnFilteredArrowEndpoints, sample_graph: GraphV2) -> None:
    result_df = knn_filtered_endpoints.stream(
        G=sample_graph,
        node_properties=["prop"],
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert set(result_df.columns) == {"node1", "node2", "similarity"}
    assert len(result_df) == 4


def test_knn_filtered_mutate(knn_filtered_endpoints: KnnFilteredArrowEndpoints, sample_graph: GraphV2) -> None:
    result = knn_filtered_endpoints.mutate(
        sample_graph,
        node_properties="prop",
        mutate_property="score",
        mutate_relationship_type="SIMILAR_TO",
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.nodes_compared >= 0
    assert result.relationships_written > 0
    assert "p50" in result.similarity_distribution
    assert result.did_converge
    assert result.ran_iterations > 0
    assert result.node_pairs_considered >= 0
    assert "concurrency" in result.configuration


@pytest.mark.db_integration
def test_knn_filtered_write(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2
) -> None:
    endpoints = KnnFilteredArrowEndpoints(
        arrow_client, write_back_client=RemoteWriteBackClient(arrow_client, query_runner), show_progress=False
    )

    result = endpoints.write(
        db_graph,
        node_properties="prop",
        write_property="score",
        write_relationship_type="SIMILAR_TO",
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.nodes_compared >= 0
    assert result.relationships_written > 0
    assert result.did_converge
    assert result.ran_iterations > 0
    assert result.node_pairs_considered >= 0
    assert "p50" in result.similarity_distribution
    assert "concurrency" in result.configuration


def test_knn_filtered_estimate(knn_filtered_endpoints: KnnFilteredArrowEndpoints, sample_graph: GraphV2) -> None:
    result = knn_filtered_endpoints.estimate(
        sample_graph,
        node_properties="prop",
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert result.required_memory is not None
    assert result.tree_view is not None
    assert result.map_view is not None
