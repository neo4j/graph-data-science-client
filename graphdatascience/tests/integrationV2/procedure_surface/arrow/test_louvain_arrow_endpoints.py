from typing import Generator

import pytest

from graphdatascience import Graph, QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.louvain_endpoints import LouvainWriteResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.louvain_arrow_endpoints import LouvainArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
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


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "louvain_g", graph) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[Graph, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        "louvain_g",
        graph,
        """
                    MATCH (n)-->(m)
                    WITH gds.graph.project.remote(n, m) as g
                    RETURN g
                """,
    ) as g:
        yield g


@pytest.fixture
def louvain_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[LouvainArrowEndpoints, None, None]:
    yield LouvainArrowEndpoints(arrow_client)


def test_louvain_stats(louvain_endpoints: LouvainArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test Louvain stats operation."""
    result = louvain_endpoints.stats(G=sample_graph)

    assert result.community_count == 2
    assert result.modularity >= 0
    assert isinstance(result.modularities, list)
    assert result.ran_levels >= 1
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert "p10" in result.community_distribution


def test_louvain_stream(louvain_endpoints: LouvainArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test Louvain stream operation."""
    result_df = louvain_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "communityId" in result_df.columns
    assert len(result_df.columns) == 2


def test_louvain_mutate(louvain_endpoints: LouvainArrowEndpoints, sample_graph: GraphV2) -> None:
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


def test_louvain_estimate(louvain_endpoints: LouvainArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test Louvain estimate operation."""
    result = louvain_endpoints.estimate(sample_graph)

    assert result.node_count == 6
    assert result.relationship_count == 6
    assert "KiB" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0


def test_louvain_stats_with_parameters(louvain_endpoints: LouvainArrowEndpoints, sample_graph: GraphV2) -> None:
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


def test_louvain_stream_with_parameters(louvain_endpoints: LouvainArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test Louvain stream operation with various parameters."""
    result_df = louvain_endpoints.stream(
        G=sample_graph,
        tolerance=0.001,
        max_levels=10,
        max_iterations=10,
        include_intermediate_communities=False,
    )

    assert "nodeId" in result_df.columns
    assert "communityId" in result_df.columns
    # When include_intermediate_communities is False, should only have 2 columns
    assert len(result_df.columns) == 2


def test_louvain_write(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: Graph) -> None:
    """Test Louvain write operation."""
    endpoints = LouvainArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))
    result = endpoints.write(G=db_graph, write_property="communityId")

    assert isinstance(result, LouvainWriteResult)
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

    assert query_runner.run_cypher("MATCH (n) WHERE n.communityId IS NOT NULL RETURN COUNT(*) AS count").squeeze() == 6


def test_louvain_mutate_with_parameters(louvain_endpoints: LouvainArrowEndpoints, sample_graph: GraphV2) -> None:
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
