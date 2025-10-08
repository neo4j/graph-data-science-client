from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.kmeans_endpoints import KMeansWriteResult
from graphdatascience.procedure_surface.arrow.community.kmeans_arrow_endpoints import KMeansArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

create_statement = """
        CREATE
            (a {kmeans: [1.0, 1.0]}),
            (b {kmeans: [1.0, 2.0]}),
            (c {kmeans: [102.0, 100.0]}),
            (d {kmeans: [100.0, 102.0]})
    """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(
        arrow_client,
        "g",
        create_statement,
    ) as g:
        yield g


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        create_statement,
        """
                MATCH (n)
                WITH gds.graph.project.remote(n, null, {sourceNodeProperties: {kmeans: n.kmeans}, targetNodeProperties: null}) as g
                RETURN g
            """,
    ) as g:
        yield g


@pytest.fixture
def kmeans_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[KMeansArrowEndpoints, None, None]:
    yield KMeansArrowEndpoints(arrow_client)


def test_kmeans_stats(kmeans_endpoints: KMeansArrowEndpoints, sample_graph: GraphV2) -> None:
    result = kmeans_endpoints.stats(G=sample_graph, node_property="kmeans", k=3)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.average_distance_to_centroid >= 0
    assert result.average_silhouette >= -1.0
    assert len(result.centroids) == 3
    assert isinstance(result.community_distribution, dict)


def test_kmeans_stream(kmeans_endpoints: KMeansArrowEndpoints, sample_graph: GraphV2) -> None:
    result_df = kmeans_endpoints.stream(
        G=sample_graph,
        node_property="kmeans",
        k=3,
    )

    assert "nodeId" in result_df.columns
    assert "communityId" in result_df.columns
    assert len(result_df) == 4


def test_kmeans_mutate(kmeans_endpoints: KMeansArrowEndpoints, sample_graph: GraphV2) -> None:
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


@pytest.mark.db_integration
def test_kmeans_write(arrow_client: AuthenticatedArrowClient, db_graph: GraphV2, query_runner: QueryRunner) -> None:
    endpoints = KMeansArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))

    result = endpoints.write(G=db_graph, node_property="kmeans", write_property="community", k=3)

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


def test_kmeans_estimate(kmeans_endpoints: KMeansArrowEndpoints, sample_graph: GraphV2) -> None:
    result = kmeans_endpoints.estimate(sample_graph, node_property="kmeans", k=3)

    assert result.node_count == 4
    assert result.relationship_count == 0
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
