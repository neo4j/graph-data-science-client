from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.catalog.graph_api import Graph
from graphdatascience.procedure_surface.api.scc_endpoints import SccMutateResult, SccStatsResult
from graphdatascience.procedure_surface.arrow.scc_arrow_endpoints import SccArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
        CREATE
              (a:Node)
            , (b:Node)
            , (c:Node)
            , (d:Node)
            , (e:Node)
            , (f:Node)
            , (g:Node)
            , (h:Node)
            , (i:Node)
            , (a)-[:TYPE {cost: 5}]->(b)
            , (b)-[:TYPE {cost: 5}]->(c)
            , (c)-[:TYPE {cost: 5}]->(a)
            , (d)-[:TYPE {cost: 2}]->(e)
            , (e)-[:TYPE {cost: 2}]->(f)
            , (f)-[:TYPE {cost: 2}]->(d)
            , (a)-[:TYPE {cost: 2}]->(d)
            , (g)-[:TYPE {cost: 3}]->(h)
            , (h)-[:TYPE {cost: 3}]->(i)
            , (i)-[:TYPE {cost: 3}]->(g)
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def scc_endpoints(arrow_client: AuthenticatedArrowClient) -> SccArrowEndpoints:
    return SccArrowEndpoints(arrow_client)


def test_scc_stats(scc_endpoints: SccArrowEndpoints, sample_graph: Graph) -> None:
    result = scc_endpoints.stats(sample_graph)

    assert isinstance(result, SccStatsResult)
    assert result.component_count == 3
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert "p10" in result.component_distribution


def test_scc_stream(scc_endpoints: SccArrowEndpoints, sample_graph: Graph) -> None:
    result = scc_endpoints.stream(sample_graph)

    assert len(result) == 9
    assert "nodeId" in result.columns
    assert "componentId" in result.columns


def test_scc_mutate(scc_endpoints: SccArrowEndpoints, sample_graph: Graph) -> None:
    result = scc_endpoints.mutate(sample_graph, "componentId")

    assert isinstance(result, SccMutateResult)
    assert result.component_count == 3
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 9
    assert "p10" in result.component_distribution


def test_scc_estimate(scc_endpoints: SccArrowEndpoints, sample_graph: Graph) -> None:
    result = scc_endpoints.estimate(G=sample_graph)

    assert result.node_count >= 0
    assert result.relationship_count >= 0
    assert result.required_memory is not None
    assert result.tree_view is not None
    assert isinstance(result.map_view, dict)
    assert result.bytes_min >= 0
    assert result.bytes_max >= 0
    assert result.heap_percentage_min >= 0
    assert result.heap_percentage_max >= 0
