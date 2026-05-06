from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.arrow.collapse_path_arrow_endpoints import CollapsePathArrowEndpoints
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
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


def test_collapse_path(collapse_path_endpoints: CollapsePathArrowEndpoints, sample_graph: GraphV2) -> None:
    result = collapse_path_endpoints.mutate(
        G=sample_graph,
        path_templates=[["REL", "REL"]],
        mutate_relationship_type="FoF",
    )

    assert result.relationshipsWritten == 3
    assert result.mutateMillis >= 0
    assert result.preProcessingMillis >= 0
    assert result.computeMillis >= 0
