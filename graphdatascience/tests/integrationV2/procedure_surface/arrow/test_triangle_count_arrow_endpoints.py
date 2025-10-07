from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.triangle_count_arrow_endpoints import TriangleCountArrowEndpoints
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
            (a)-[:REL]->(c),
            (b)-[:REL]->(c),
            (d)-[:REL]->(e),
            (d)-[:REL]->(f),
            (e)-[:REL]->(f),
            (a)-[:REL]->(d)
        """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "g", graph, undirected=("REL", "UNDIRECTED_REL")) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        graph,
        """
                    MATCH (n)-[r]->(m)
                    WITH gds.graph.project.remote(n, m) as g
                    RETURN g
                """,
        undirected_relationship_types=["*"],
    ) as g:
        yield g


@pytest.fixture
def triangle_count_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[TriangleCountArrowEndpoints, None, None]:
    yield TriangleCountArrowEndpoints(arrow_client, show_progress=False)


@pytest.fixture
def triangle_count_endpoints_with_write_back(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner
) -> Generator[TriangleCountArrowEndpoints, None, None]:
    yield TriangleCountArrowEndpoints(
        arrow_client, write_back_client=RemoteWriteBackClient(arrow_client, query_runner), show_progress=False
    )


def test_triangle_count_stats(triangle_count_endpoints: TriangleCountArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test Triangle Count stats operation via Arrow."""
    result = triangle_count_endpoints.stats(G=sample_graph)

    assert result.global_triangle_count >= 0
    assert result.node_count == 6
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0


def test_triangle_count_stream(triangle_count_endpoints: TriangleCountArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test Triangle Count stream operation via Arrow."""
    result_df = triangle_count_endpoints.stream(G=sample_graph)

    assert "nodeId" in result_df.columns
    assert "triangleCount" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 6  # We have 6 nodes

    # Check that triangle counts are non-negative
    assert all(result_df["triangleCount"] >= 0)


def test_triangle_count_mutate(triangle_count_endpoints: TriangleCountArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test Triangle Count mutate operation via Arrow."""
    result = triangle_count_endpoints.mutate(
        G=sample_graph,
        mutate_property="triangle_count",
    )

    assert result.global_triangle_count >= 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 6
    assert result.node_count == 6


def test_triangle_count_write(
    triangle_count_endpoints_with_write_back: TriangleCountArrowEndpoints, db_graph: GraphV2
) -> None:
    """Test Triangle Count write operation via Arrow."""
    result = triangle_count_endpoints_with_write_back.write(
        G=db_graph,
        write_property="triangle_count_write",
    )

    assert result.global_triangle_count >= 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 6
    assert result.node_count == 6


def test_triangle_count_estimate(triangle_count_endpoints: TriangleCountArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test Triangle Count estimate operation via Arrow."""
    result = triangle_count_endpoints.estimate(sample_graph)

    assert result.node_count == 6
    assert result.relationship_count == 14
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
