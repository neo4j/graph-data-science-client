from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.arrow.similarity.node_similarity_arrow_endpoints import (
    NodeSimilarityArrowEndpoints,
)
from graphdatascience.query_runner import QueryRunner
from graphdatascience.session.remote_ops.write_protocols import WriteProtocol
from tests.integration.procedure_surface.arrow.graph_creation_helper import (
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
        (d)-[:REL]->(a),
        (a)-[:REL]->(c),
        (b)-[:REL]->(d)
        """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    with create_graph(arrow_client, "g", graph, undirected=("REL", "REL_UNDIRECTED")) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[Graph, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        graph,
        """
                    MATCH (n)--(m)
                    WITH gds.graph.project.remote(n, m) as g
                    RETURN g
                """,
    ) as g:
        yield g


@pytest.fixture
def node_similarity_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[NodeSimilarityArrowEndpoints, None, None]:
    yield NodeSimilarityArrowEndpoints(arrow_client)


def test_node_similarity_stats(node_similarity_endpoints: NodeSimilarityArrowEndpoints, sample_graph: Graph) -> None:
    result = node_similarity_endpoints.stats(G=sample_graph, top_k=2)

    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.nodes_compared > 0
    assert result.similarity_pairs > 0
    assert "p50" in result.similarity_distribution


def test_node_similarity_stream(node_similarity_endpoints: NodeSimilarityArrowEndpoints, sample_graph: Graph) -> None:
    result_df = node_similarity_endpoints.stream(
        G=sample_graph,
        top_k=2,
    )

    assert set(result_df.columns) == {"node1", "node2", "similarity"}
    assert len(result_df) > 0


def test_node_similarity_mutate(node_similarity_endpoints: NodeSimilarityArrowEndpoints, sample_graph: Graph) -> None:
    result = node_similarity_endpoints.mutate(
        G=sample_graph,
        mutate_relationship_type="SIMILAR",
        mutate_property="similarity",
        top_k=2,
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.relationships_written > 0
    assert result.nodes_compared > 0


@pytest.mark.db_integration
def test_node_similarity_write(
    arrow_client: AuthenticatedArrowClient,
    db_graph: Graph,
    query_runner: QueryRunner,
) -> None:
    endpoints_with_writeback = NodeSimilarityArrowEndpoints(
        arrow_client=arrow_client,
        write_protocol=WriteProtocol.select(arrow_client, query_runner),
    )

    result = endpoints_with_writeback.write(
        G=db_graph,
        write_relationship_type="SIMILAR",
        write_property="similarity",
        top_k=2,
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.relationships_written > 0
    assert result.nodes_compared > 0


def test_node_similarity_estimate(node_similarity_endpoints: NodeSimilarityArrowEndpoints, sample_graph: Graph) -> None:
    result = node_similarity_endpoints.estimate(sample_graph, top_k=2)

    assert result.node_count == 4
    assert result.relationship_count == 12
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0


def test_compute_summary_and_stream(
    node_similarity_endpoints: NodeSimilarityArrowEndpoints, sample_graph: Graph
) -> None:
    handle = node_similarity_endpoints.compute(G=sample_graph, top_k=2)
    summary = handle.summary()
    assert summary["nodesCompared"] > 0
    assert "p50" in summary["similarityDistribution"]

    df = handle.stream()
    assert set(df.columns) == {"node1", "node2", "similarity"}
    assert len(df) > 0
