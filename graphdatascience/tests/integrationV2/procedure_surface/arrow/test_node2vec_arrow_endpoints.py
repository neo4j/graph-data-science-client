import json
from typing import Generator

import pytest

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.node2vec_arrow_endpoints import Node2VecArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    CREATE
    (a: Node),
    (b: Node),
    (c: Node),
    (a)-[:REL]->(c),
    (b)-[:REL]->(c)
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def node2vec_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[Node2VecArrowEndpoints, None, None]:
    yield Node2VecArrowEndpoints(arrow_client)


def test_node2vec_mutate(node2vec_endpoints: Node2VecArrowEndpoints, sample_graph: Graph) -> None:
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


def test_node2vec_stream(node2vec_endpoints: Node2VecArrowEndpoints, sample_graph: Graph) -> None:
    """Test Node2Vec stream operation."""
    result = node2vec_endpoints.stream(
        G=sample_graph,
        embedding_dimension=64,
        walks_per_node=1,
        walk_length=2,
    )

    assert len(result) == 3  # We have 3 nodes
    assert set(result.columns) == {"nodeId", "embedding"}


def test_node2vec_write(node2vec_endpoints: Node2VecArrowEndpoints, sample_graph: Graph) -> None:
    """Test Node2Vec write operation."""
    with pytest.raises(Exception, match="Write back client is not initialized"):
        node2vec_endpoints.write(
            G=sample_graph,
            write_property="node2vec_embedding",
            embedding_dimension=64,
            walks_per_node=1,
            walk_length=2,
        )


def test_node2vec_estimate(node2vec_endpoints: Node2VecArrowEndpoints, sample_graph: Graph) -> None:
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
