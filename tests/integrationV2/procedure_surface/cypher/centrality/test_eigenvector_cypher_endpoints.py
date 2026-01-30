from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.centrality.eigenvector_cypher_endpoints import EigenvectorCypherEndpoints
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node {name: 'a'}),
    (b: Node {name: 'b'}),
    (c: Node {name: 'c'}),
    (d: Node {name: 'd'}),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d),
    (d)-[:REL]->(a)
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
def eigenvector_endpoints(query_runner: QueryRunner) -> Generator[EigenvectorCypherEndpoints, None, None]:
    yield EigenvectorCypherEndpoints(query_runner)


def test_eigenvector_stats(eigenvector_endpoints: EigenvectorCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Eigenvector stats operation."""
    result = eigenvector_endpoints.stats(G=sample_graph)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.ran_iterations >= 1
    assert isinstance(result.did_converge, bool)
    assert "p50" in result.centrality_distribution


def test_eigenvector_stream(eigenvector_endpoints: EigenvectorCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Eigenvector stream operation."""
    result_df = eigenvector_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "score" in result_df.columns
    assert len(result_df.columns) == 2


def test_eigenvector_mutate(eigenvector_endpoints: EigenvectorCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Eigenvector mutate operation."""
    result = eigenvector_endpoints.mutate(
        G=sample_graph,
        mutate_property="eigenvector",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 4
    assert result.ran_iterations >= 1
    assert isinstance(result.did_converge, bool)
    assert "p50" in result.centrality_distribution


def test_eigenvector_estimate(eigenvector_endpoints: EigenvectorCypherEndpoints, sample_graph: GraphV2) -> None:
    result = eigenvector_endpoints.estimate(sample_graph)

    assert result.node_count == 4
    assert result.relationship_count == 4
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
