from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.centrality.bridges_arrow_endpoints import BridgesArrowEndpoints
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
)

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
    with create_graph(arrow_client, "g", graph, undirected=("REL", "REL_UNDIRECTED")) as G:
        yield G


@pytest.fixture
def bridges_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[BridgesArrowEndpoints, None, None]:
    yield BridgesArrowEndpoints(arrow_client, show_progress=False)


def test_bridges_stream(bridges_endpoints: BridgesArrowEndpoints, sample_graph: GraphV2) -> None:
    """Test Bridges stream operation."""
    result_df = bridges_endpoints.stream(G=sample_graph)

    assert set(result_df.columns) == {"from", "to", "remainingSizes"}
    assert len(result_df) == 2


def test_bridges_estimate(bridges_endpoints: BridgesArrowEndpoints, sample_graph: GraphV2) -> None:
    result = bridges_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 4
    assert "Bytes" in result.required_memory
