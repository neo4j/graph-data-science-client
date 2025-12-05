from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.community.hdbscan_arrow_endpoints import HdbscanArrowEndpoints
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
        CREATE
            (a:Node {id: 1, prop: [1.0, 2.0]}),
            (b:Node {id: 2, prop: [2.0, 3.0]}),
            (c:Node {id: 3, prop: [3.0, 4.0]}),
            (d:Node {id: 4, prop: [4.0, 5.0]})
    """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(
        arrow_client,
        "hdbscan_test_graph",
        graph,
    ) as g:
        yield g


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        graph,
        """
                    MATCH (n)
                    WITH gds.graph.project.remote(n, null, {sourceNodeProperties: properties(n), targetNodeProperties: null}) as g
                    RETURN g
                """,
    ) as g:
        yield g


@pytest.fixture
def hdbscan_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[HdbscanArrowEndpoints, None, None]:
    yield HdbscanArrowEndpoints(arrow_client)


def test_hdbscan_stats(hdbscan_endpoints: HdbscanArrowEndpoints, sample_graph: GraphV2) -> None:
    result = hdbscan_endpoints.stats(G=sample_graph, node_property="prop", min_cluster_size=2)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.number_of_clusters >= 0
    assert "concurrency" in result.configuration


def test_hdbscan_stream(hdbscan_endpoints: HdbscanArrowEndpoints, sample_graph: GraphV2) -> None:
    result_df = hdbscan_endpoints.stream(
        G=sample_graph,
        node_property="prop",
    )

    assert set(result_df.columns) == {"nodeId", "label"}
    assert len(result_df) == 4


def test_hdbscan_mutate(hdbscan_endpoints: HdbscanArrowEndpoints, sample_graph: GraphV2) -> None:
    result = hdbscan_endpoints.mutate(
        G=sample_graph,
        node_property="prop",
        mutate_property="hdbscanCluster",
    )

    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.number_of_clusters >= 0
    assert "concurrency" in result.configuration


@pytest.mark.db_integration
def test_hdbscan_write(arrow_client: AuthenticatedArrowClient, db_graph: GraphV2, query_runner: QueryRunner) -> None:
    endpoints = HdbscanArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))
    result = endpoints.write(
        G=db_graph,
        node_property="prop",
        min_cluster_size=2,
        write_property="hdbscanCluster",
    )

    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.number_of_clusters >= 0
    assert result.node_count > 0
    assert result.node_properties_written > 0
    assert "concurrency" in result.configuration


def test_hdbscan_estimate(hdbscan_endpoints: HdbscanArrowEndpoints, sample_graph: GraphV2) -> None:
    result = hdbscan_endpoints.estimate(
        G=sample_graph,
        node_property="prop",
        min_cluster_size=2,
    )

    assert result.node_count == 4
    assert result.relationship_count == 0
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
