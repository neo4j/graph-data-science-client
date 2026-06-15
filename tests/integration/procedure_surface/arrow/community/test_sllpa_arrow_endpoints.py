from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.community.sllpa_endpoints import SllpaWriteResult
from graphdatascience.procedure_surface.arrow.community.sllpa_arrow_endpoints import SllpaArrowEndpoints
from graphdatascience.query_runner import QueryRunner
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol
from tests.integration.procedure_surface.arrow.graph_creation_helper import (
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
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    with create_graph(arrow_client, "g", graph) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[Graph, None, None]:
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
def sllpa_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[SllpaArrowEndpoints, None, None]:
    yield SllpaArrowEndpoints(arrow_client, show_progress=False)


def test_sllpa_stats(sllpa_endpoints: SllpaArrowEndpoints, sample_graph: Graph) -> None:
    """Test SLLPA stats operation via Arrow."""
    result = sllpa_endpoints.stats(G=sample_graph, max_iterations=1)

    assert result.ran_iterations > 0
    assert result.did_converge in [True, False]
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert isinstance(result.configuration, dict)


def test_sllpa_stream(sllpa_endpoints: SllpaArrowEndpoints, sample_graph: Graph) -> None:
    """Test SLLPA stream operation via Arrow."""
    result_df = sllpa_endpoints.stream(G=sample_graph, max_iterations=1)

    assert len(result_df) == 6  # 6 nodes in the graph
    assert "nodeId" in result_df.columns
    assert "community" in result_df.columns


def test_sllpa_mutate(sllpa_endpoints: SllpaArrowEndpoints, sample_graph: Graph) -> None:
    """Test SLLPA mutate operation via Arrow."""
    result = sllpa_endpoints.mutate(
        G=sample_graph,
        max_iterations=1,
        mutate_property="sllpa_community",
    )

    assert result.ran_iterations > 0
    assert result.did_converge in [True, False]
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 6
    assert isinstance(result.configuration, dict)


@pytest.mark.db_integration
def test_sllpa_write(arrow_client: AuthenticatedArrowClient, db_graph: Graph, query_runner: QueryRunner) -> None:
    """Test SLLPA write operation via Arrow."""

    endpoints = SllpaArrowEndpoints(arrow_client, WriteProtocol.select(arrow_client, query_runner))

    result = endpoints.write(
        G=db_graph,
        max_iterations=1,
        write_property="sllpa_community_write",
    )

    assert isinstance(result, SllpaWriteResult)
    assert result.ran_iterations > 0
    assert result.did_converge in [True, False]
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 6
    assert isinstance(result.configuration, dict)


def test_sllpa_estimate(sllpa_endpoints: SllpaArrowEndpoints, sample_graph: Graph) -> None:
    """Test SLLPA estimate operation via Arrow."""
    result = sllpa_endpoints.estimate(sample_graph, max_iterations=1)

    assert result.node_count == 6
    assert result.relationship_count == 7
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0


def test_compute(sllpa_endpoints: SllpaArrowEndpoints, sample_graph: Graph) -> None:
    handle = sllpa_endpoints.compute(G=sample_graph, max_iterations=1)
    summary = handle.summary()

    assert summary["ranIterations"] > 0
    assert summary["computeMillis"] >= 0
    assert "writeProperty" not in summary["configuration"]

    df = handle.stream()
    assert "nodeId" in df.columns
    assert "community" in df.columns
    assert len(df) == 6
