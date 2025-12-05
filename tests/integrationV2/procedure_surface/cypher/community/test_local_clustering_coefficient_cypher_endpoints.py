from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.community.local_clustering_coefficient_cypher_endpoints import (
    LocalClusteringCoefficientCypherEndpoints,
)
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node),
    (b: Node),
    (c: Node),
    (d: Node),
    (e: Node),
    (a)-[:REL]->(b),
    (a)-[:REL]->(c),
    (b)-[:REL]->(c),
    (c)-[:REL]->(d),
    (d)-[:REL]->(e)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {}, {undirectedRelationshipTypes: ['*']}) AS G
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
def local_clustering_coefficient_endpoints(
    query_runner: QueryRunner,
) -> Generator[LocalClusteringCoefficientCypherEndpoints, None, None]:
    yield LocalClusteringCoefficientCypherEndpoints(query_runner)


def test_local_clustering_coefficient_stats(
    local_clustering_coefficient_endpoints: LocalClusteringCoefficientCypherEndpoints, sample_graph: GraphV2
) -> None:
    """Test Local Clustering Coefficient stats operation."""
    result = local_clustering_coefficient_endpoints.stats(G=sample_graph)

    assert result.node_count == 5
    assert result.average_clustering_coefficient >= 0.0
    assert result.average_clustering_coefficient <= 1.0
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0


def test_local_clustering_coefficient_stream(
    local_clustering_coefficient_endpoints: LocalClusteringCoefficientCypherEndpoints, sample_graph: GraphV2
) -> None:
    """Test Local Clustering Coefficient stream operation."""
    result_df = local_clustering_coefficient_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "localClusteringCoefficient" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 5  # We have 5 nodes


def test_local_clustering_coefficient_mutate(
    local_clustering_coefficient_endpoints: LocalClusteringCoefficientCypherEndpoints, sample_graph: GraphV2
) -> None:
    """Test Local Clustering Coefficient mutate operation."""
    result = local_clustering_coefficient_endpoints.mutate(
        G=sample_graph,
        mutate_property="localClusteringCoefficient",
    )

    assert result.node_count == 5
    assert result.node_properties_written == 5
    assert result.average_clustering_coefficient >= 0.0
    assert result.average_clustering_coefficient <= 1.0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0


def test_local_clustering_coefficient_estimate(
    local_clustering_coefficient_endpoints: LocalClusteringCoefficientCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = local_clustering_coefficient_endpoints.estimate(sample_graph)

    assert result.node_count == 5
    assert result.relationship_count == 10
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
