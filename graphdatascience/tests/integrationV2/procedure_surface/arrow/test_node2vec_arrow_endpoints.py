from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.node2vec_endpoints import Node2VecWriteResult
from graphdatascience.procedure_surface.arrow.node2vec_arrow_endpoints import Node2VecArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
        CREATE
            (a: Node),
            (b: Node),
            (c: Node),
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
def node2vec_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[Node2VecArrowEndpoints, None, None]:
    yield Node2VecArrowEndpoints(arrow_client)


def test_node2vec_mutate(node2vec_endpoints: Node2VecArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test Node2Vec mutate operation."""
    result = node2vec_endpoints.mutate(
        G=sample_graph,
        mutate_property="node2vec_embedding",
        embedding_dimension=64,
        walks_per_node=1,
        walk_length=2,
    )

    assert result.node_count == 3
    assert result.node_properties_written == 3
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.configuration is not None
    assert isinstance(result.loss_per_iteration, list)


def test_node2vec_stream(node2vec_endpoints: Node2VecArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test Node2Vec stream operation."""
    result = node2vec_endpoints.stream(
        G=sample_graph,
        embedding_dimension=64,
        walks_per_node=1,
        walk_length=2,
    )

    assert len(result) == 3  # We have 3 nodes
    assert set(result.columns) == {"nodeId", "embedding"}


def test_node2vec_write(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2) -> None:
    """Test Node2Vec write operation."""
    endpoints = Node2VecArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))
    result = endpoints.write(
        G=db_graph,
        write_property="node2vec_embedding",
        embedding_dimension=64,
        walks_per_node=1,
        walk_length=2,
    )

    assert isinstance(result, Node2VecWriteResult)
    assert result.node_count == 3
    assert result.node_properties_written == 3
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.configuration is not None
    assert isinstance(result.loss_per_iteration, list)

    assert (
        query_runner.run_cypher("MATCH (n) WHERE n.node2vec_embedding IS NOT NULL RETURN COUNT(*) AS count").squeeze()
        == 3
    )


def test_node2vec_write_without_write_back_client(
    node2vec_endpoints: Node2VecArrowEndpoints, sample_graph: GraphV2
) -> None:
    """Test Node2Vec write operation without write back client."""
    with pytest.raises(Exception, match="Write back client is not initialized"):
        node2vec_endpoints.write(
            G=sample_graph,
            write_property="node2vec_embedding",
            embedding_dimension=64,
            walks_per_node=1,
            walk_length=2,
        )


def test_node2vec_estimate(node2vec_endpoints: Node2VecArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test Node2Vec estimate operation."""
    result = node2vec_endpoints.estimate(
        G=sample_graph,
        embedding_dimension=64,
        walks_per_node=1,
        walk_length=2,
    )

    assert result.node_count == 3
    assert result.relationship_count == 2
    assert "KiB" in result.required_memory or "Bytes" in result.required_memory
