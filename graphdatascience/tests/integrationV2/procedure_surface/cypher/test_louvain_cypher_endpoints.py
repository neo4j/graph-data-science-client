from typing import Generator

import pytest

from graphdatascience import Graph, QueryRunner
from graphdatascience.procedure_surface.cypher.louvain_cypher_endpoints import LouvainCypherEndpoints


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_statement = """
    CREATE
    (a: Node),
    (b: Node),
    (c: Node),
    (d: Node),
    (e: Node),
    (f: Node),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c),
    (c)-[:REL]->(a),
    (d)-[:REL]->(e),
    (e)-[:REL]->(f),
    (f)-[:REL]->(d)
    """

    query_runner.run_cypher(create_statement)

    query_runner.run_cypher("""
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('louvain_g', n, m, {}) AS G
        RETURN G
    """)

    yield Graph("louvain_g", query_runner)

    query_runner.run_cypher("CALL gds.graph.drop('louvain_g')")
    query_runner.run_cypher("MATCH (n) DETACH DELETE n")


@pytest.fixture
def louvain_endpoints(query_runner: QueryRunner) -> Generator[LouvainCypherEndpoints, None, None]:
    yield LouvainCypherEndpoints(query_runner)


def test_louvain_stats(louvain_endpoints: LouvainCypherEndpoints, sample_graph: Graph) -> None:
    """Test Louvain stats operation."""
    result = louvain_endpoints.stats(G=sample_graph)

    assert result.community_count >= 1
    assert result.modularity >= 0
    assert isinstance(result.modularities, list)
    assert result.ran_levels >= 1
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert "p10" in result.community_distribution


def test_louvain_stream(louvain_endpoints: LouvainCypherEndpoints, sample_graph: Graph) -> None:
    """Test Louvain stream operation."""
    result_df = louvain_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "communityId" in result_df.columns
    assert len(result_df.columns) == 3


def test_louvain_mutate(louvain_endpoints: LouvainCypherEndpoints, sample_graph: Graph) -> None:
    """Test Louvain mutate operation."""
    result = louvain_endpoints.mutate(
        G=sample_graph,
        mutate_property="communityId",
    )

    assert result.community_count >= 1
    assert result.modularity >= 0
    assert isinstance(result.modularities, list)
    assert result.ran_levels >= 1
    assert "p10" in result.community_distribution
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 6


def test_louvain_write(louvain_endpoints: LouvainCypherEndpoints, sample_graph: Graph) -> None:
    """Test Louvain write operation."""
    result = louvain_endpoints.write(
        G=sample_graph,
        write_property="communityId",
    )

    assert result.community_count >= 1
    assert result.modularity >= 0
    assert isinstance(result.modularities, list)
    assert result.ran_levels >= 1
    assert "p10" in result.community_distribution
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 6


def test_louvain_estimate(louvain_endpoints: LouvainCypherEndpoints, sample_graph: Graph) -> None:
    """Test Louvain estimate operation."""
    result = louvain_endpoints.estimate(sample_graph)

    assert result.node_count == 6
    assert result.relationship_count == 6
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0


def test_louvain_stats_with_parameters(louvain_endpoints: LouvainCypherEndpoints, sample_graph: Graph) -> None:
    """Test Louvain stats operation with various parameters."""
    result = louvain_endpoints.stats(
        G=sample_graph,
        tolerance=0.001,
        max_levels=10,
        max_iterations=10,
        include_intermediate_communities=True,
    )

    assert result.community_count >= 1
    assert result.modularity >= 0
    assert isinstance(result.modularities, list)
    assert result.ran_levels >= 1
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert "p10" in result.community_distribution


def test_louvain_stream_with_parameters(louvain_endpoints: LouvainCypherEndpoints, sample_graph: Graph) -> None:
    """Test Louvain stream operation with various parameters."""
    result_df = louvain_endpoints.stream(
        G=sample_graph,
        tolerance=0.001,
        max_levels=10,
        max_iterations=10,
        include_intermediate_communities=False,
        min_community_size=1,
    )

    assert "nodeId" in result_df.columns
    assert "communityId" in result_df.columns
    # When include_intermediate_communities is False, should only have 2 columns
    assert len(result_df.columns) == 2


def test_louvain_mutate_with_parameters(louvain_endpoints: LouvainCypherEndpoints, sample_graph: Graph) -> None:
    """Test Louvain mutate operation with various parameters."""
    result = louvain_endpoints.mutate(
        G=sample_graph,
        mutate_property="louvainCommunity",
        tolerance=0.001,
        max_levels=5,
        max_iterations=10,
        consecutive_ids=True,
    )

    assert result.community_count >= 1
    assert result.modularity >= 0
    assert isinstance(result.modularities, list)
    assert result.ran_levels >= 1
    assert "p10" in result.community_distribution
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 6


def test_louvain_write_with_parameters(louvain_endpoints: LouvainCypherEndpoints, sample_graph: Graph) -> None:
    """Test Louvain write operation with various parameters."""
    result = louvain_endpoints.write(
        G=sample_graph,
        write_property="louvainCommunity",
        tolerance=0.001,
        max_levels=5,
        max_iterations=10,
        consecutive_ids=True,
        write_concurrency=2,
        min_community_size=1,
    )

    assert result.community_count >= 1
    assert result.modularity >= 0
    assert isinstance(result.modularities, list)
    assert result.ran_levels >= 1
    assert "p10" in result.community_distribution
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 6
