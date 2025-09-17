from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import Graph
from graphdatascience.procedure_surface.cypher.node2vec_cypher_endpoints import Node2VecCypherEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_statement = """
    CREATE
    (a: Node),
    (b: Node),
    (c: Node),
    (a)-[:REL]->(c),
    (b)-[:REL]->(c)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {}) AS G
        RETURN G
    """

    with create_graph(
        query_runner,
        "g",
        create_statement,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def node2vec_endpoints(query_runner: QueryRunner) -> Generator[Node2VecCypherEndpoints, None, None]:
    yield Node2VecCypherEndpoints(query_runner)


def test_node2vec_mutate(node2vec_endpoints: Node2VecCypherEndpoints, sample_graph: Graph) -> None:
    """Test Node2Vec mutate operation."""
    result = node2vec_endpoints.mutate(
        G=sample_graph,
        mutate_property="node2vec_embedding",
        embedding_dimension=64,
        walks_per_node=10,
        walk_length=80,
    )

    assert result.node_count == 3
    assert result.node_properties_written == 3
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.configuration is not None
    assert isinstance(result.loss_per_iteration, list)


def test_node2vec_stream(node2vec_endpoints: Node2VecCypherEndpoints, sample_graph: Graph) -> None:
    """Test Node2Vec stream operation."""
    result = node2vec_endpoints.stream(
        G=sample_graph,
        embedding_dimension=64,
        walks_per_node=10,
        walk_length=80,
    )

    assert len(result) == 3  # We have 3 nodes
    assert set(result.columns) == {"nodeId", "embedding"}


def test_node2vec_write(node2vec_endpoints: Node2VecCypherEndpoints, sample_graph: Graph) -> None:
    """Test Node2Vec write operation."""
    result = node2vec_endpoints.write(
        G=sample_graph,
        write_property="node2vec_embedding",
        embedding_dimension=64,
        walks_per_node=10,
        walk_length=80,
    )

    assert result.node_count == 3
    assert result.node_properties_written == 3
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.configuration is not None
    assert isinstance(result.loss_per_iteration, list)


def test_node2vec_estimate(node2vec_endpoints: Node2VecCypherEndpoints, sample_graph: Graph) -> None:
    """Test Node2Vec estimate operation."""
    result = node2vec_endpoints.estimate(
        G=sample_graph,
        embedding_dimension=64,
        walks_per_node=10,
        walk_length=80,
    )

    assert result.node_count == 3
    assert result.relationship_count == 2
    assert "KiB" in result.required_memory or "Bytes" in result.required_memory
