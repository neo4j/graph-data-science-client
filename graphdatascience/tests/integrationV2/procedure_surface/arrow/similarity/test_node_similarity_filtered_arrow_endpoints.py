from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.similarity.node_similarity_filtered_arrow_endpoints import (
    NodeSimilarityFilteredArrowEndpoints,
)
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
    CREATE
        (a: SourceNode),
        (b: SourceNode),
        (c: TargetNode),
        (d: TargetNode),
        (a)-[:REL]->(b),
        (b)-[:REL]->(c),
        (c)-[:REL]->(d),
        (d)-[:REL]->(a),
        (a)-[:REL]->(c),
        (b)-[:REL]->(d)
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
                    MATCH (n)--(m)
                    WITH gds.graph.project.remote(n, m, {sourceNodeLabels: labels(n), targetNodeLabels: labels(m)}) as g
                    RETURN g
                """,
    ) as g:
        yield g


@pytest.fixture
def node_similarity_filtered_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[NodeSimilarityFilteredArrowEndpoints, None, None]:
    yield NodeSimilarityFilteredArrowEndpoints(arrow_client)


def test_node_similarity_filtered_stats(
    node_similarity_filtered_endpoints: NodeSimilarityFilteredArrowEndpoints, sample_graph: GraphV2
) -> None:
    """Test NodeSimilarity filtered stats operation."""
    result = node_similarity_filtered_endpoints.stats(
        G=sample_graph,
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.nodes_compared > 0
    assert result.similarity_pairs > 0
    assert "p50" in result.similarity_distribution


def test_node_similarity_filtered_stream(
    node_similarity_filtered_endpoints: NodeSimilarityFilteredArrowEndpoints, sample_graph: GraphV2
) -> None:
    """Test NodeSimilarity filtered stream operation."""
    result_df = node_similarity_filtered_endpoints.stream(
        G=sample_graph,
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert set(result_df.columns) == {"node1", "node2", "similarity"}
    assert len(result_df) >= 1


def test_node_similarity_filtered_mutate(
    node_similarity_filtered_endpoints: NodeSimilarityFilteredArrowEndpoints, sample_graph: GraphV2
) -> None:
    """Test NodeSimilarity filtered mutate operation."""
    result = node_similarity_filtered_endpoints.mutate(
        G=sample_graph,
        mutate_relationship_type="SIMILAR",
        mutate_property="similarity",
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.relationships_written > 0
    assert result.nodes_compared > 0


@pytest.mark.db_integration
def test_node_similarity_filtered_write(
    arrow_client: AuthenticatedArrowClient,
    db_graph: GraphV2,
    query_runner: QueryRunner,
) -> None:
    """Test NodeSimilarity filtered write operation."""
    endpoints_with_writeback = NodeSimilarityFilteredArrowEndpoints(
        arrow_client=arrow_client,
        write_back_client=RemoteWriteBackClient(arrow_client, query_runner),
    )

    result = endpoints_with_writeback.write(
        G=db_graph,
        write_relationship_type="SIMILAR",
        write_property="similarity",
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.relationships_written > 0
    assert result.nodes_compared > 0


def test_node_similarity_filtered_estimate(
    node_similarity_filtered_endpoints: NodeSimilarityFilteredArrowEndpoints, sample_graph: GraphV2
) -> None:
    """Test NodeSimilarity filtered estimate operation."""
    result = node_similarity_filtered_endpoints.estimate(
        sample_graph,
        top_k=2,
        source_node_filter="SourceNode",
        target_node_filter="TargetNode",
    )

    assert result.node_count == 4
    assert result.relationship_count == 6  # Number of relationships in the graph
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
