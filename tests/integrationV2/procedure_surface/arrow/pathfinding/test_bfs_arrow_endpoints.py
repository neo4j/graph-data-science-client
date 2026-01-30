from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.pathfinding.bfs_arrow_endpoints import BFSArrowEndpoints
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph

graph = """
        CREATE
            (a:Node {id: 0}),
            (b:Node {id: 1}),
            (c:Node {id: 2}),
            (a)-[:REL]->(b),
            (b)-[:REL]->(c)
        """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "g", graph) as G:
        yield G


@pytest.fixture
def bfs_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[BFSArrowEndpoints, None, None]:
    yield BFSArrowEndpoints(arrow_client, show_progress=False)


def test_bfs_stream(bfs_endpoints: BFSArrowEndpoints, sample_graph: GraphV2) -> None:
    result_df = bfs_endpoints.stream(G=sample_graph, source_node=0)

    assert set(result_df.columns) == {"sourceNode", "nodeIds"}
    assert len(result_df) == 1


def test_bfs_mutate(bfs_endpoints: BFSArrowEndpoints, sample_graph: GraphV2) -> None:
    result = bfs_endpoints.mutate(G=sample_graph, source_node=0, mutate_relationship_type="BFS_REL")

    assert result.relationships_written == 2


def test_bfs_estimate(bfs_endpoints: BFSArrowEndpoints, sample_graph: GraphV2) -> None:
    result = bfs_endpoints.estimate(sample_graph, source_node=0)

    assert result.node_count == 3
    assert result.relationship_count == 2
    assert "Bytes" in result.required_memory


def test_bfs_stats(bfs_endpoints: BFSArrowEndpoints, sample_graph: GraphV2) -> None:
    result = bfs_endpoints.stats(G=sample_graph, source_node=0)

    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
