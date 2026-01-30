from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.community.maxkcut_cypher_endpoints import MaxKCutCypherEndpoints
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
    (f: Node),
    (a)-[:REL {weight: 1.0}]->(b),
    (a)-[:REL {weight: 1.0}]->(c),
    (b)-[:REL {weight: 1.0}]->(c),
    (d)-[:REL {weight: 1.0}]->(e),
    (d)-[:REL {weight: 1.0}]->(f),
    (e)-[:REL {weight: 1.0}]->(f),
    (a)-[:REL {weight: 0.1}]->(d),
    (b)-[:REL {weight: 0.1}]->(e),
    (c)-[:REL {weight: 0.1}]->(f)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {relationshipProperties: r {.weight}}) AS G
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
def maxkcut_endpoints(query_runner: QueryRunner) -> Generator[MaxKCutCypherEndpoints, None, None]:
    yield MaxKCutCypherEndpoints(query_runner)


def test_maxkcut_stream(maxkcut_endpoints: MaxKCutCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Approximate Maximum k-cut stream operation."""
    result_df = maxkcut_endpoints.stream(
        G=sample_graph,
        k=2,
    )

    assert "nodeId" in result_df.columns
    assert "communityId" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 6  # We have 6 nodes

    # Check that we have exactly 2 communities (k=2)
    unique_communities = result_df["communityId"].nunique()
    assert unique_communities <= 2  # Should be at most k communities


def test_maxkcut_mutate(maxkcut_endpoints: MaxKCutCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Approximate Maximum k-cut mutate operation."""
    result = maxkcut_endpoints.mutate(
        G=sample_graph,
        mutate_property="maxkcut_community",
        k=2,
    )

    assert result.cut_cost >= 0.0  # Cut cost should be non-negative
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 6  # All 6 nodes should have community assigned


def test_maxkcut_estimate(maxkcut_endpoints: MaxKCutCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Approximate Maximum k-cut estimate operation."""
    result = maxkcut_endpoints.estimate(sample_graph, k=2)

    assert result.node_count == 6
    assert result.relationship_count == 9
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
