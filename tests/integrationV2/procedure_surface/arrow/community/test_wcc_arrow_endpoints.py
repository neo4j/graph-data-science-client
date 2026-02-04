from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.wcc_endpoints import WccWriteResult
from graphdatascience.procedure_surface.arrow.community.wcc_arrow_endpoints import WccArrowEndpoints
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
        CREATE
            (a: Node),
            (b: Node),
            (c: Node),
            (a)-[:REL]->(c)
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
                    OPTIONAL MATCH (n)-->(m)
                    WITH gds.graph.project.remote(n, m) as g
                    RETURN g
                """,
    ) as g:
        yield g


@pytest.fixture
def wcc_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[WccArrowEndpoints, None, None]:
    yield WccArrowEndpoints(arrow_client)


def test_wcc_stats(wcc_endpoints: WccArrowEndpoints, sample_graph: GraphV2) -> None:
    result = wcc_endpoints.stats(G=sample_graph)

    assert result.component_count == 2
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert "p10" in result.component_distribution


def test_wcc_stream(wcc_endpoints: WccArrowEndpoints, sample_graph: GraphV2) -> None:
    result_df = wcc_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "componentId" in result_df.columns
    assert len(result_df.columns) == 2


def test_wcc_mutate(wcc_endpoints: WccArrowEndpoints, sample_graph: GraphV2) -> None:
    result = wcc_endpoints.mutate(
        G=sample_graph,
        mutate_property="componentId",
    )

    assert result.component_count == 2
    assert "p10" in result.component_distribution
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 3


@pytest.mark.db_integration
def test_wcc_write(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2) -> None:
    endpoints = WccArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))
    result = endpoints.write(G=db_graph, write_property="componentId")

    assert isinstance(result, WccWriteResult)
    assert result.component_count == 2
    assert "p10" in result.component_distribution
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 3

    assert query_runner.run_cypher("MATCH (n) WHERE n.componentId IS NOT NULL RETURN COUNT(*) AS count").squeeze() == 3


def test_wcc_estimate(wcc_endpoints: WccArrowEndpoints, sample_graph: GraphV2) -> None:
    result = wcc_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 1
    assert "Bytes" in result.required_memory
    # assert result.tree_view > 0
    # assert result.map_view > 0
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0


def test_wcc_compute_and_mutate(wcc_endpoints: WccArrowEndpoints, sample_graph: GraphV2) -> None:
    job = wcc_endpoints.compute(
        G=sample_graph,
    )

    assert job.progress() >= 0

    job.wait()

    summary = job.result()

    assert summary.component_count == 2
    assert "p10" in summary.component_distribution
    assert summary.pre_processing_millis >= 0
    assert summary.compute_millis >= 0
    assert summary.post_processing_millis >= 0
