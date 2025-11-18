from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.community.conductance_arrow_endpoints import ConductanceArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph

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
def conductance_endpoints(arrow_client: AuthenticatedArrowClient) -> ConductanceArrowEndpoints:
    return ConductanceArrowEndpoints(arrow_client)


def test_conductance_stream(conductance_endpoints: ConductanceArrowEndpoints, sample_graph: GraphV2) -> None:
    result = conductance_endpoints.stream(sample_graph, "community")

    assert set(result.columns) == {"community", "conductance"}
    assert len(result) == 2
