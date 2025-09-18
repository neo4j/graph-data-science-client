from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.kcore_endpoints import KCoreWriteResult
from graphdatascience.procedure_surface.arrow.kcore_arrow_endpoints import KCoreArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
        CREATE
            (a: Node),
            (b: Node),
            (c: Node),
            (d: Node),
            (e: Node),
            (f: Node),
            (a)-[:REL]->(b),
            (b)-[:REL]->(c),
            (c)-[:REL]->(a),
            (d)-[:REL]->(e),
            (e)-[:REL]->(f),
            (f)-[:REL]->(d),
            (a)-[:REL]->(d)
        """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "kcore_g", graph, ("REL", "REL2")) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        "kcore_g",
        graph,
        """
                    MATCH (n)-->(m)
                    WITH gds.graph.project.remote(n, m, {relationshipType: "REL"}) as g
                    RETURN g
                """,
        ["REL"],
    ) as g:
        yield g


@pytest.fixture
def kcore_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[KCoreArrowEndpoints, None, None]:
    yield KCoreArrowEndpoints(arrow_client)


def test_kcore_stats(kcore_endpoints: KCoreArrowEndpoints, sample_graph: GraphV2) -> None:
    result = kcore_endpoints.stats(G=sample_graph)

    assert result.degeneracy >= 1
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.pre_processing_millis >= 0


def test_kcore_stream(kcore_endpoints: KCoreArrowEndpoints, sample_graph: GraphV2) -> None:
    result_df = kcore_endpoints.stream(G=sample_graph)

    assert "nodeId" in result_df.columns
    assert "coreValue" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 6


def test_kcore_mutate(kcore_endpoints: KCoreArrowEndpoints, sample_graph: GraphV2) -> None:
    result = kcore_endpoints.mutate(G=sample_graph, mutate_property="coreValue")

    assert result.degeneracy >= 1
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 6


def test_kcore_estimate(kcore_endpoints: KCoreArrowEndpoints, sample_graph: GraphV2) -> None:
    result = kcore_endpoints.estimate(sample_graph)

    assert result.node_count == 6
    assert result.relationship_count == 14
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0


def test_kcore_stats_with_parameters(kcore_endpoints: KCoreArrowEndpoints, sample_graph: GraphV2) -> None:
    result = kcore_endpoints.stats(G=sample_graph, relationship_types=["REL2"], concurrency=2)

    assert result.degeneracy >= 1
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0


def test_kcore_stream_with_parameters(kcore_endpoints: KCoreArrowEndpoints, sample_graph: GraphV2) -> None:
    result_df = kcore_endpoints.stream(G=sample_graph, relationship_types=["REL2"], concurrency=2)

    assert "nodeId" in result_df.columns
    assert "coreValue" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 6


def test_kcore_mutate_with_parameters(kcore_endpoints: KCoreArrowEndpoints, sample_graph: GraphV2) -> None:
    result = kcore_endpoints.mutate(
        G=sample_graph, mutate_property="kcoreValue", relationship_types=["REL2"], concurrency=2
    )

    assert result.degeneracy >= 1
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 6


def test_kcore_write(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2) -> None:
    endpoints = KCoreArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))
    result = endpoints.write(G=db_graph, write_property="coreValue")

    assert isinstance(result, KCoreWriteResult)
    assert result.degeneracy >= 1
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 6

    assert query_runner.run_cypher("MATCH (n) WHERE n.coreValue IS NOT NULL RETURN COUNT(*) AS count").squeeze() == 6


def test_kcore_write_without_write_back_client(kcore_endpoints: KCoreArrowEndpoints, sample_graph: GraphV2) -> None:
    with pytest.raises(Exception, match="Write back client is not initialized"):
        kcore_endpoints.write(
            G=sample_graph,
            write_property="coreValue",
        )
