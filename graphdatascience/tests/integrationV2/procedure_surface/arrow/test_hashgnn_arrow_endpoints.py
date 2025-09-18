from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.hashgnn_endpoints import HashGNNWriteResult
from graphdatascience.procedure_surface.arrow.hashgnn_arrow_endpoints import HashGNNArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    gdl = """
    CREATE
    (a: Node {feature: [1L, 0L, 1L, 0L]}),
    (b: Node {feature: [0L, 1L, 0L, 1L]}),
    (c: Node {feature: [1L, 1L, 0L, 0L]}),
    (d: Node {feature: [0L, 0L, 1L, 1L]}),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d),
    (d)-[:REL]->(a)
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    graph = """
        CREATE
            (a: Node {feature: [1, 0, 1, 0]}),
            (b: Node {feature: [0, 1, 0, 1]}),
            (c: Node {feature: [1, 1, 0, 0]}),
            (d: Node {feature: [0, 0, 1, 1]}),
            (a)-[:REL]->(b),
            (b)-[:REL]->(c),
            (c)-[:REL]->(d),
            (d)-[:REL]->(a)
        """

    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        graph,
        """
                    MATCH (n)-->(m)
                    WITH gds.graph.project.remote(
                        n,
                        m,
                        {sourceNodeProperties: properties(n), targetNodeProperties: properties(m)}
                    ) as g
                    RETURN g
                """,
    ) as g:
        yield g


@pytest.fixture
def hashgnn_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[HashGNNArrowEndpoints, None, None]:
    yield HashGNNArrowEndpoints(arrow_client)


def test_hashgnn_mutate(hashgnn_endpoints: HashGNNArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test HashGNN mutate operation."""
    result = hashgnn_endpoints.mutate(
        G=sample_graph,
        iterations=2,
        embedding_density=256,
        mutate_property="hashgnn_embedding",
        feature_properties=["feature"],
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 4
    assert result.configuration is not None


def test_hashgnn_stream(hashgnn_endpoints: HashGNNArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test HashGNN stream operation."""
    result = hashgnn_endpoints.stream(
        G=sample_graph,
        iterations=2,
        embedding_density=4,
        feature_properties=["feature"],
    )

    assert len(result) == 4  # We have 4 nodes
    assert set(result.columns) == {"nodeId", "embedding"}


def test_hashgnn_write(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2) -> None:
    """Test HashGNN write operation."""
    endpoints = HashGNNArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))
    result = endpoints.write(
        G=db_graph,
        iterations=2,
        embedding_density=64,
        write_property="hashgnn_embedding",
        feature_properties=["feature"],
    )

    assert isinstance(result, HashGNNWriteResult)
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 4
    assert result.configuration is not None

    assert (
        query_runner.run_cypher("MATCH (n) WHERE n.hashgnn_embedding IS NOT NULL RETURN COUNT(*) AS count").squeeze()
        == 4
    )


def test_hashgnn_write_without_write_back_client(
    hashgnn_endpoints: HashGNNArrowEndpoints, sample_graph: GraphV2
) -> None:
    """Test HashGNN write operation without write back client."""
    with pytest.raises(Exception, match="Write back client is not initialized"):
        hashgnn_endpoints.write(
            G=sample_graph,
            iterations=2,
            embedding_density=64,
            write_property="hashgnn_embedding",
            feature_properties=["feature"],
        )


def test_hashgnn_estimate(hashgnn_endpoints: HashGNNArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test HashGNN estimate operation."""
    result = hashgnn_endpoints.estimate(
        G=sample_graph,
        iterations=2,
        embedding_density=4,
        feature_properties=["feature"],
    )

    assert result.node_count == 4
    assert result.relationship_count == 4
    assert "KiB" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
