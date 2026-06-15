from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.arrow.community.triangles_arrow_endpoints import (
    TrianglesArrowEndpoints,
)
from tests.integration.procedure_surface.arrow.graph_creation_helper import create_graph

graph = """
        CREATE
            (a: Node),
            (b: Node),
            (c: Node),
            (d: Node),
            (e: Node),
            (f: Node),
            (a)-[:REL]->(b),
            (a)-[:REL]->(c),
            (b)-[:REL]->(c),
            (d)-[:REL]->(e),
            (d)-[:REL]->(f),
            (e)-[:REL]->(f),
            (a)-[:REL]->(d)
        """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    with create_graph(arrow_client, "g", graph, undirected=("REL", "UNDIRECTED_REL")) as G:
        yield G


def test_triangles_happy_path(arrow_client: AuthenticatedArrowClient, sample_graph: Graph) -> None:
    result_df = TrianglesArrowEndpoints(arrow_client, show_progress=False)(G=sample_graph)

    assert list(result_df.columns) == ["nodeA", "nodeB", "nodeC"]
    assert len(result_df) > 0
