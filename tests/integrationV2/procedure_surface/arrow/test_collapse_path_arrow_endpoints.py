from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.arrow.collapse_path_arrow_endpoints import CollapsePathArrowEndpoints
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
        CREATE
        (a:Node),
        (b:Node),
        (c:Node),
        (a)-[:REL {weight: 1.0}]->(b),
        (b)-[:REL {weight: 2.0}]->(c),
        (c)-[:REL {weight: 3.0}]->(a),
        (a)-[:OTHER {value: 10}]->(c)
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def collapse_path_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[CollapsePathArrowEndpoints, None, None]:
    yield CollapsePathArrowEndpoints(arrow_client)


def test_collapse_path(collapse_path_endpoints: CollapsePathArrowEndpoints, sample_graph: Graph) -> None:
    result = collapse_path_endpoints.mutate(
        G=sample_graph,
        path_templates=[["REL", "REL"]],
        mutate_relationship_type="FoF",
    )

    assert result.relationships_written == 3
    assert result.mutate_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
