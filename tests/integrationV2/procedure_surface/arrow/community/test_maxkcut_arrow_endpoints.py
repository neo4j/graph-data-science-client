from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.community.maxkcut_arrow_endpoints import MaxKCutArrowEndpoints
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
)

graph = """
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


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "g", graph) as G:
        yield G


@pytest.fixture
def maxkcut_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[MaxKCutArrowEndpoints, None, None]:
    yield MaxKCutArrowEndpoints(arrow_client, show_progress=False)


@pytest.fixture
def maxkcut_endpoints_with_write_back(
    arrow_client: AuthenticatedArrowClient, write_back_client: RemoteWriteBackClient
) -> Generator[MaxKCutArrowEndpoints, None, None]:
    yield MaxKCutArrowEndpoints(arrow_client, write_back_client, show_progress=False)


def test_maxkcut_stream(maxkcut_endpoints: MaxKCutArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test Approximate Maximum k-cut stream operation via Arrow."""
    result_df = maxkcut_endpoints.stream(
        G=sample_graph,
        k=2,
    )

    assert "nodeId" in result_df.columns
    assert "communityId" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 6  # We have 6 nodes

    # Check that we have at most k communities
    unique_communities = result_df["communityId"].nunique()
    assert unique_communities <= 2


def test_maxkcut_mutate(maxkcut_endpoints: MaxKCutArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test Approximate Maximum k-cut mutate operation via Arrow."""
    result = maxkcut_endpoints.mutate(
        G=sample_graph,
        mutate_property="maxkcut_community",
        k=2,
    )

    assert result.cut_cost >= 0.0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 6


def test_maxkcut_estimate(maxkcut_endpoints: MaxKCutArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test Approximate Maximum k-cut estimate operation via Arrow."""
    result = maxkcut_endpoints.estimate(sample_graph, k=2)

    assert result.node_count == 6
    assert result.relationship_count == 9
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
