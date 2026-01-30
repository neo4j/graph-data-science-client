from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.community.modularity_arrow_endpoints import ModularityArrowEndpoints
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph

graph = """
        CREATE
              (a:Node {community: 0}),
              (b:Node {community: 0}),
              (c:Node {community: 0}),
              (d:Node {community: 1}),
              (e:Node {community: 1}),
              (f:Node {community: 1}),
              (a)-[:TYPE {weight: 1.0}]->(b),
              (b)-[:TYPE {weight: 1.0}]->(c),
              (c)-[:TYPE {weight: 1.0}]->(a),
              (d)-[:TYPE {weight: 1.0}]->(e),
              (e)-[:TYPE {weight: 1.0}]->(f),
              (f)-[:TYPE {weight: 1.0}]->(d),
              (a)-[:TYPE {weight: 2.0}]->(d),
              (c)-[:TYPE {weight: 2.0}]->(e)
        """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "g", graph) as G:
        yield G


@pytest.fixture
def modularity_endpoints(arrow_client: AuthenticatedArrowClient) -> ModularityArrowEndpoints:
    return ModularityArrowEndpoints(arrow_client)


def test_modularity_stats(modularity_endpoints: ModularityArrowEndpoints, sample_graph: GraphV2) -> None:
    result = modularity_endpoints.stats(sample_graph, "community")

    assert result.community_count == 2
    assert result.modularity == 0.21875
    assert result.node_count == 6
    assert result.relationship_count == 8


def test_modularity_stream(modularity_endpoints: ModularityArrowEndpoints, sample_graph: GraphV2) -> None:
    result = modularity_endpoints.stream(sample_graph, "community")

    assert set(result.columns) == {"communityId", "modularity"}
    assert len(result) == 2


def test_modularity_estimate(modularity_endpoints: ModularityArrowEndpoints, sample_graph: GraphV2) -> None:
    result = modularity_endpoints.estimate(sample_graph, "community")

    assert result.bytes_min > 0
    assert result.bytes_max >= result.bytes_min
