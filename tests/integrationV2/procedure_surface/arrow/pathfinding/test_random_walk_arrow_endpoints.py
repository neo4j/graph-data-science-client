from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.pathfinding.random_walk_arrow_endpoints import (
    RandomWalkArrowEndpoints,
)
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph

graph = """
        CREATE
            (a:Node {id: 0}),
            (b:Node {id: 1}),
            (c:Node {id: 2}),
            (a)-[:REL]->(b),
            (b)-[:REL]->(c),
            (c)-[:REL]->(a)
        """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "g", graph) as G:
        yield G


@pytest.fixture
def random_walk_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[RandomWalkArrowEndpoints, None, None]:
    yield RandomWalkArrowEndpoints(arrow_client, show_progress=False)


def test_random_walk_stream(random_walk_endpoints: RandomWalkArrowEndpoints, sample_graph: GraphV2) -> None:
    result_df = random_walk_endpoints.stream(G=sample_graph, walks_per_node=2, walk_length=4, random_seed=42)

    assert len(result_df) > 0


def test_random_walk_mutate(random_walk_endpoints: RandomWalkArrowEndpoints, sample_graph: GraphV2) -> None:
    with pytest.raises(Exception, match="Mutation is not supported for RandomWalk"):
        random_walk_endpoints.mutate(sample_graph, "mutate", walks_per_node=2, walk_length=4, random_seed=42)


def test_random_walk_stats(random_walk_endpoints: RandomWalkArrowEndpoints, sample_graph: GraphV2) -> None:
    result = random_walk_endpoints.stats(G=sample_graph, walks_per_node=1, walk_length=3, random_seed=42)

    assert result.compute_millis >= 0


def test_random_walk_estimate(random_walk_endpoints: RandomWalkArrowEndpoints, sample_graph: GraphV2) -> None:
    result = random_walk_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 3
    assert "Bytes" in result.required_memory


def test_compute(random_walk_endpoints: RandomWalkArrowEndpoints, sample_graph: GraphV2) -> None:
    handle = random_walk_endpoints.compute(G=sample_graph, walks_per_node=1, walk_length=3, random_seed=42)
    summary = handle.summary()

    assert summary["computeMillis"] >= 0

    df = handle.stream()
    assert set(df.columns) == {"walk"}
    assert len(df) == 3
