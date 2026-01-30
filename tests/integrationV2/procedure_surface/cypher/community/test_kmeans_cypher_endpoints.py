from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.kmeans_endpoints import KMeansWriteResult
from graphdatascience.procedure_surface.cypher.community.kmeans_cypher_endpoints import KMeansCypherEndpoints
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
        CREATE
            (a {kmeans: [1.0, 1.0]}),
            (b {kmeans: [1.0, 2.0]}),
            (c {kmeans: [102.0, 100.0]}),
            (d {kmeans: [100.0, 102.0]})
    """

    projection_query = """
        MATCH (n)
        WITH gds.graph.project("g", n, null, {sourceNodeProperties: {kmeans: n.kmeans}, targetNodeProperties: null}) as g
        RETURN g
    """

    with create_graph(
        query_runner,
        "g",
        create_statement,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def kmeans_endpoints(query_runner: QueryRunner) -> Generator[KMeansCypherEndpoints, None, None]:
    yield KMeansCypherEndpoints(query_runner)


def test_kmeans_stats(kmeans_endpoints: KMeansCypherEndpoints, sample_graph: GraphV2) -> None:
    result = kmeans_endpoints.stats(G=sample_graph, node_property="kmeans", k=3)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.average_distance_to_centroid >= 0
    assert result.average_silhouette >= -1.0
    assert len(result.centroids) == 3
    assert isinstance(result.community_distribution, dict)


def test_kmeans_stream(kmeans_endpoints: KMeansCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test K-Means stream operation."""
    result_df = kmeans_endpoints.stream(
        G=sample_graph,
        node_property="kmeans",
        k=3,
    )

    assert "nodeId" in result_df.columns
    assert "communityId" in result_df.columns
    assert "distanceFromCentroid" in result_df.columns
    assert "silhouette" in result_df.columns
    assert len(result_df.columns) == 4


def test_kmeans_mutate(kmeans_endpoints: KMeansCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test K-Means mutate operation."""
    result = kmeans_endpoints.mutate(
        G=sample_graph,
        node_property="kmeans",
        mutate_property="community",
        k=3,
    )

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 4
    assert result.average_distance_to_centroid >= 0
    assert result.average_silhouette >= -1.0
    assert len(result.centroids) == 3
    assert isinstance(result.community_distribution, dict)


def test_kmeans_write(
    kmeans_endpoints: KMeansCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    result = kmeans_endpoints.write(G=sample_graph, node_property="kmeans", write_property="community", k=3)

    assert isinstance(result, KMeansWriteResult)
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 4
    assert result.average_distance_to_centroid >= 0
    assert result.average_silhouette >= -1.0
    assert len(result.centroids) == 3
    assert isinstance(result.community_distribution, dict)

    assert query_runner.run_cypher("MATCH (n) WHERE n.community IS NOT NULL RETURN COUNT(*) AS count").squeeze() == 4


def test_kmeans_estimate(kmeans_endpoints: KMeansCypherEndpoints, sample_graph: GraphV2) -> None:
    result = kmeans_endpoints.estimate(sample_graph, node_property="kmeans", k=3)

    assert result.node_count == 4
    assert result.relationship_count == 0
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
