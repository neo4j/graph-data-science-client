from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.arrow.pathfinding.dfs_arrow_endpoints import DFSArrowEndpoints
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
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    with create_graph(arrow_client, "g", graph) as G:
        yield G


@pytest.fixture
def dfs_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[DFSArrowEndpoints, None, None]:
    yield DFSArrowEndpoints(arrow_client, show_progress=False)


def test_dfs_stream(dfs_endpoints: DFSArrowEndpoints, sample_graph: Graph) -> None:
    result_df = dfs_endpoints.stream(G=sample_graph, source_node=0)

    assert set(result_df.columns) == {"sourceNode", "nodeIds"}
    assert len(result_df) == 1


def test_dfs_mutate(dfs_endpoints: DFSArrowEndpoints, sample_graph: Graph) -> None:
    result = dfs_endpoints.mutate(G=sample_graph, source_node=0, mutate_relationship_type="DFS_REL")

    assert result.relationships_written == 2


def test_dfs_estimate(dfs_endpoints: DFSArrowEndpoints, sample_graph: Graph) -> None:
    result = dfs_endpoints.estimate(sample_graph, source_node=0)

    assert result.node_count == 3
    assert result.relationship_count == 2
    assert "Bytes" in result.required_memory


def test_dfs_stats(dfs_endpoints: DFSArrowEndpoints, sample_graph: Graph) -> None:
    result = dfs_endpoints.stats(G=sample_graph, source_node=0)

    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0


def test_compute(dfs_endpoints: DFSArrowEndpoints, sample_graph: Graph) -> None:
    handle = dfs_endpoints.compute(G=sample_graph, source_node=0)
    summary = handle.summary()

    assert summary["computeMillis"] >= 0
    assert "writeProperty" not in summary["configuration"]

    df = handle.stream()
    assert set(df.columns) == {"sourceNode", "nodeIds"}
