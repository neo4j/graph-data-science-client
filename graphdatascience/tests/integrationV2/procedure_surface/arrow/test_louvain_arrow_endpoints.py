import json
from typing import Generator

import pytest

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.louvain_arrow_endpoints import LouvainArrowEndpoints


class MockGraph(Graph):
    def __init__(self, name: str):
        self._name = name

    def name(self) -> str:
        return self._name


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    (a: Node)
    (b: Node)
    (c: Node)
    (d: Node)
    (e: Node)
    (f: Node)
    (a)-[:REL]->(b)
    (b)-[:REL]->(c)
    (c)-[:REL]->(a)
    (d)-[:REL]->(e)
    (e)-[:REL]->(f)
    (f)-[:REL]->(d)
    """

    arrow_client.do_action("v2/graph.fromGDL", json.dumps({"graphName": "louvain_g", "gdlGraph": gdl}).encode("utf-8"))
    yield MockGraph("louvain_g")
    arrow_client.do_action("v2/graph.drop", json.dumps({"graphName": "louvain_g"}).encode("utf-8"))


@pytest.fixture
def louvain_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[LouvainArrowEndpoints, None, None]:
    yield LouvainArrowEndpoints(arrow_client)


def test_louvain_stats(louvain_endpoints: LouvainArrowEndpoints, sample_graph: Graph) -> None:
    """Test Louvain stats operation."""
    result = louvain_endpoints.stats(G=sample_graph)

    assert result.community_count == 2
    assert result.modularity >= 0
    assert isinstance(result.modularities, list)
    assert result.ran_levels >= 1
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
    assert "p10" in result.community_distribution


def test_louvain_stream(louvain_endpoints: LouvainArrowEndpoints, sample_graph: Graph) -> None:
    """Test Louvain stream operation."""
    result_df = louvain_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "communityId" in result_df.columns
    assert len(result_df.columns) == 2


def test_louvain_mutate(louvain_endpoints: LouvainArrowEndpoints, sample_graph: Graph) -> None:
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


def test_louvain_estimate(louvain_endpoints: LouvainArrowEndpoints, sample_graph: Graph) -> None:
    """Test Louvain estimate operation."""
    result = louvain_endpoints.estimate(sample_graph)

    assert result.node_count == 6
    assert result.relationship_count == 6
    assert "KiB" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0


def test_louvain_stats_with_parameters(louvain_endpoints: LouvainArrowEndpoints, sample_graph: Graph) -> None:
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


def test_louvain_stream_with_parameters(louvain_endpoints: LouvainArrowEndpoints, sample_graph: Graph) -> None:
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


def test_louvain_mutate_with_parameters(louvain_endpoints: LouvainArrowEndpoints, sample_graph: Graph) -> None:
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
