from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.degree_endpoints import DegreeWriteResult
from graphdatascience.procedure_surface.arrow.degree_arrow_endpoints import DegreeArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
        CREATE
            (a: Node {id: 0}),
            (b: Node {id: 1}),
            (c: Node {id: 2}),
            (a)-[:REL]->(c),
            (b)-[:REL]->(c)
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
                    MATCH (n)-->(m)
                    WITH gds.graph.project.remote(n, m) as g
                    RETURN g
                """,
    ) as g:
        yield g


@pytest.fixture
def degree_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[DegreeArrowEndpoints, None, None]:
    yield DegreeArrowEndpoints(arrow_client)


def test_degree_stats(degree_endpoints: DegreeArrowEndpoints, sample_graph: GraphV2) -> None:
    result = degree_endpoints.stats(G=sample_graph)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert "p50" in result.centrality_distribution
    assert isinstance(result.configuration, dict)


def test_degree_stream(degree_endpoints: DegreeArrowEndpoints, sample_graph: GraphV2) -> None:
    result_df = degree_endpoints.stream(G=sample_graph)

    assert "nodeId" in result_df.columns
    assert "score" in result_df.columns
    assert len(result_df) == 3  # We have 3 nodes
    assert all(result_df["score"] >= 0)  # Degree scores should be non-negative


def test_degree_mutate(degree_endpoints: DegreeArrowEndpoints, sample_graph: GraphV2) -> None:
    result = degree_endpoints.mutate(G=sample_graph, mutate_property="degree")

    assert result.node_properties_written == 3
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert "p50" in result.centrality_distribution
    assert isinstance(result.configuration, dict)


@pytest.mark.db_integration
def test_degree_write(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2) -> None:
    endpoints = DegreeArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))
    result = endpoints.write(G=db_graph, write_property="degree")

    assert isinstance(result, DegreeWriteResult)
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 3
    assert "p50" in result.centrality_distribution

    assert query_runner.run_cypher("MATCH (n) WHERE n.degree IS NOT NULL RETURN COUNT(*) AS count").squeeze() == 3


def test_degree_estimate(degree_endpoints: DegreeArrowEndpoints, sample_graph: GraphV2) -> None:
    result = degree_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 2
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
