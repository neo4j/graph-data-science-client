from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.node_embedding.fastrp_endpoints import FastRPWriteResult
from graphdatascience.procedure_surface.arrow.node_embedding.fastrp_arrow_endpoints import FastRPArrowEndpoints
from graphdatascience.query_runner import QueryRunner, QueryType
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
        CREATE
            (a: Node),
            (b: Node),
            (c: Node),
            (d: Node),
            (a)-[:REL]->(b),
            (b)-[:REL]->(c),
            (c)-[:REL]->(d),
            (d)-[:REL]->(a)
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
def fastrp_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[FastRPArrowEndpoints, None, None]:
    yield FastRPArrowEndpoints(arrow_client)


def test_fastrp_stats(fastrp_endpoints: FastRPArrowEndpoints, sample_graph: Graph) -> None:
    """Test FastRP stats operation."""
    result = fastrp_endpoints.stats(G=sample_graph, embedding_dimension=128)

    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.configuration is not None


def test_fastrp_stream(fastrp_endpoints: FastRPArrowEndpoints, sample_graph: Graph) -> None:
    """Test FastRP stream operation."""
    result_df = fastrp_endpoints.stream(
        G=sample_graph,
        embedding_dimension=64,
    )

    assert set(result_df.columns) == {"nodeId", "embedding"}
    assert len(result_df.columns) == 2
    assert len(result_df) == 4  # We have 4 nodes

    # Check that embeddings have the correct dimension
    embedding_sample = result_df["embedding"].iloc[0]
    assert len(embedding_sample) == 64


def test_fastrp_mutate(fastrp_endpoints: FastRPArrowEndpoints, sample_graph: Graph) -> None:
    """Test FastRP mutate operation."""
    result = fastrp_endpoints.mutate(
        G=sample_graph,
        mutate_property="fastrp_embedding",
        embedding_dimension=32,
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 4
    assert result.configuration is not None


@pytest.mark.db_integration
def test_fastrp_write(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: Graph) -> None:
    endpoints = FastRPArrowEndpoints(arrow_client, WriteProtocol.select(arrow_client, query_runner))
    result = endpoints.write(G=db_graph, write_property="fastrp_embedding", embedding_dimension=32)

    assert isinstance(result, FastRPWriteResult)
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 4
    assert result.configuration is not None

    assert (
        query_runner.run_cypher(
            "MATCH (n) WHERE n.fastrp_embedding IS NOT NULL RETURN COUNT(*) AS count", query_type=QueryType.USER_ACTION
        ).iloc[0, 0]
        == 4
    )


def test_fastrp_write_without_write_back_client(fastrp_endpoints: FastRPArrowEndpoints, sample_graph: Graph) -> None:
    with pytest.raises(Exception, match="Write back is not supported by this session."):
        fastrp_endpoints.write(
            G=sample_graph,
            write_property="fastrp_embedding",
            embedding_dimension=2,
        )


def test_fastrp_estimate(fastrp_endpoints: FastRPArrowEndpoints, sample_graph: Graph) -> None:
    """Test FastRP estimate operation."""
    result = fastrp_endpoints.estimate(sample_graph, embedding_dimension=128)

    assert result.node_count == 4
    assert result.relationship_count == 4
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0


def test_compute(fastrp_endpoints: FastRPArrowEndpoints, sample_graph: Graph) -> None:
    handle = fastrp_endpoints.compute(G=sample_graph, embedding_dimension=64)
    summary = handle.summary()

    assert summary["computeMillis"] >= 0
    assert "writeProperty" not in summary["configuration"]

    df = handle.stream()
    assert set(df.columns) == {"nodeId", "embedding"}
    assert len(df) == 4
    assert len(df["embedding"].iloc[0]) == 64
