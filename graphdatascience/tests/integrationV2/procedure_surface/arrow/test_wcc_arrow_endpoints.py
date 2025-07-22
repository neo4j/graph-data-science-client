import json

import pytest

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize_single
from graphdatascience.procedure_surface.arrow.arrow_wcc_endpoints import WccArrowEndpoints


class MockGraph(Graph):
    def __init__(self, name: str):
        self._name = name

    def name(self) -> str:
        return self._name


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient):
    gdl = """
    (a: Node)
    (b: Node)
    (c: Node)
    (a)-[:REL]->(c)
    """

    res = arrow_client.do_action( "v2/graph.fromGDL", json.dumps({"graphName": "g", "gdlGraph": gdl}).encode("utf-8"))
    print(deserialize_single(res))
    yield MockGraph("g")
    arrow_client.do_action( "v2/graph.drop", json.dumps({"graphName": "g"}).encode("utf-8"))

@pytest.fixture
def wcc_endpoints(arrow_client: AuthenticatedArrowClient):
    yield WccArrowEndpoints(arrow_client)


def test_wcc_stats(wcc_endpoints: WccArrowEndpoints, sample_graph: Graph):
    """Test WCC stats operation."""
    result = wcc_endpoints.stats(
        G=sample_graph
    )

    assert result.component_count == 2
    assert result.compute_millis > 0
    assert result.pre_processing_millis > 0
    assert result.post_processing_millis > 0
    assert "p10" in result.component_distribution

def test_wcc_stream(wcc_endpoints: WccArrowEndpoints, sample_graph: Graph):
    """Test WCC stream operation."""
    result_df = wcc_endpoints.stream(
        G=sample_graph,
    )

    assert "nodeId" in result_df.columns
    assert "componentId" in result_df.columns
    assert len(result_df.columns) == 2

def test_wcc_mutate(wcc_endpoints: WccArrowEndpoints, sample_graph: Graph):
    """Test WCC mutate operation."""
    result = wcc_endpoints.mutate(
        G=sample_graph,
        mutate_property="componentId",
    )

    assert result.component_count == 2
    assert "p10" in result.component_distribution
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 3
