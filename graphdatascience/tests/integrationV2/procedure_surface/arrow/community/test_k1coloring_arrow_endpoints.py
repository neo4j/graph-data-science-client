from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.k1coloring_endpoints import K1ColoringWriteResult
from graphdatascience.procedure_surface.arrow.community.k1coloring_arrow_endpoints import K1ColoringArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
        CREATE
            (a: Node),
            (b: Node),
            (c: Node),
            (a)-[:REL]->(b),
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
def k1coloring_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[K1ColoringArrowEndpoints, None, None]:
    yield K1ColoringArrowEndpoints(arrow_client)


def test_k1coloring_stats(k1coloring_endpoints: K1ColoringArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test K1Coloring stats operation."""
    result = k1coloring_endpoints.stats(G=sample_graph)

    assert result.color_count == 3
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.ran_iterations >= 1
    assert result.did_converge
    assert isinstance(result.did_converge, bool)


def test_k1coloring_stream(k1coloring_endpoints: K1ColoringArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test K1Coloring stream operation."""
    result_df = k1coloring_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "color" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 3


def test_k1coloring_mutate(k1coloring_endpoints: K1ColoringArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test K1Coloring mutate operation."""
    result = k1coloring_endpoints.mutate(
        G=sample_graph,
        mutate_property="color",
    )

    assert result.color_count == 3
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_count == 3
    assert result.ran_iterations >= 1
    assert result.did_converge
    assert isinstance(result.did_converge, bool)


@pytest.mark.db_integration
def test_k1coloring_write(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2) -> None:
    endpoints = K1ColoringArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))
    result = endpoints.write(G=db_graph, write_property="color")

    assert isinstance(result, K1ColoringWriteResult)
    assert result.color_count == 3
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.ran_iterations >= 1
    assert result.did_converge
    assert isinstance(result.did_converge, bool)

    assert query_runner.run_cypher("MATCH (n) WHERE n.color IS NOT NULL RETURN COUNT(*) AS count").squeeze() == 3


def test_k1coloring_estimate(k1coloring_endpoints: K1ColoringArrowEndpoints, sample_graph: GraphV2) -> None:
    result = k1coloring_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 3
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
