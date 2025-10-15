from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.community.hdbscan_cypher_endpoints import HdbscanCypherEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
        CREATE
            (a:Node {id: 1, prop: [1.0, 2.0]}),
            (b:Node {id: 2, prop: [2.0, 3.0]}),
            (c:Node {id: 3, prop: [3.0, 4.0]}),
            (d:Node {id: 4, prop: [4.0, 5.0]})
    """

    projection_query = """
        MATCH (n:Node)
        WITH gds.graph.project("hdbscan_test_graph", n, null, {sourceNodeProperties: properties(n), targetNodeProperties: null}) as g
        RETURN g
    """

    with create_graph(
        query_runner,
        "hdbscan_test_graph",
        create_statement,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def hdbscan_endpoints(query_runner: QueryRunner) -> Generator[HdbscanCypherEndpoints, None, None]:
    yield HdbscanCypherEndpoints(query_runner)


def test_hdbscan_stats(hdbscan_endpoints: HdbscanCypherEndpoints, sample_graph: GraphV2) -> None:
    result = hdbscan_endpoints.stats(G=sample_graph, node_property="prop")

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.number_of_clusters >= 0
    assert "concurrency" in result.configuration


def test_hdbscan_stream(hdbscan_endpoints: HdbscanCypherEndpoints, sample_graph: GraphV2) -> None:
    result_df = hdbscan_endpoints.stream(
        G=sample_graph,
        node_property="prop",
    )

    assert set(result_df.columns) == {"nodeId", "label"}
    assert len(result_df) == 4


def test_hdbscan_mutate(hdbscan_endpoints: HdbscanCypherEndpoints, sample_graph: GraphV2) -> None:
    result = hdbscan_endpoints.mutate(
        G=sample_graph,
        node_property="prop",
        min_cluster_size=2,
        mutate_property="hdbscanCluster",
    )

    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.number_of_clusters >= 0
    assert result.node_properties_written > 0
    assert result.node_count > 0
    assert "concurrency" in result.configuration


def test_hdbscan_write(hdbscan_endpoints: HdbscanCypherEndpoints, sample_graph: GraphV2) -> None:
    result = hdbscan_endpoints.write(
        G=sample_graph,
        node_property="prop",
        min_cluster_size=2,
        write_property="hdbscanCluster",
    )

    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.number_of_clusters >= 0
    assert result.node_properties_written > 0
    assert result.node_count > 0
    assert "concurrency" in result.configuration


def test_hdbscan_estimate(hdbscan_endpoints: HdbscanCypherEndpoints, sample_graph: GraphV2) -> None:
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
