from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.pathfinding.all_shortest_path_arrow_endpoints import (
    AllShortestPathArrowEndpoints,
)
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph

graph = """
        CREATE
              (a:Node),
              (b:Node),
              (c:Node),
              (d:Node),
              (e:Node),
              (a)-[:TYPE {cost: 1.0}]->(b),
              (b)-[:TYPE {cost: 1.0}]->(c),
              (a)-[:TYPE {cost: 2.0}]->(c),
              (c)-[:TYPE {cost: 1.0}]->(d),
              (d)-[:TYPE {cost: 1.0}]->(e)
        """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "g", graph) as G:
        yield G


@pytest.fixture
def all_shortest_path_endpoints(arrow_client: AuthenticatedArrowClient) -> AllShortestPathArrowEndpoints:
    return AllShortestPathArrowEndpoints(arrow_client)


def test_all_shortest_paths_stream(
    all_shortest_path_endpoints: AllShortestPathArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = all_shortest_path_endpoints.stream(sample_graph)

    assert len(result) > 0
    assert set(result.columns) == {"sourceNodeId", "targetNodeId", "distance"}


def test_all_shortest_paths_estimate(
    all_shortest_path_endpoints: AllShortestPathArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = all_shortest_path_endpoints.estimate(G=sample_graph)

    assert result.node_count >= 0
    assert result.relationship_count >= 0
    assert result.required_memory is not None
    assert result.tree_view is not None
    assert isinstance(result.map_view, dict)
    assert result.bytes_min >= 0
    assert result.bytes_max >= 0
    assert result.heap_percentage_min >= 0
    assert result.heap_percentage_max >= 0
