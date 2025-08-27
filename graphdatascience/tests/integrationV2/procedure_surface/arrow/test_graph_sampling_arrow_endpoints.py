from typing import Generator

import pytest

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.graph_sampling_arrow_endpoints import GraphSamplingArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
)


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    (a :Node {id: 0})
    (b :Node {id: 1})
    (c :Node {id: 2})
    (d :Node {id: 3})
    (e :Node {id: 4})
    (a)-[:REL {weight: 1.0}]->(b)
    (b)-[:REL {weight: 2.0}]->(c)
    (c)-[:REL {weight: 1.5}]->(d)
    (d)-[:REL {weight: 0.5}]->(e)
    (e)-[:REL {weight: 1.2}]->(a)
    """

    yield create_graph(arrow_client, "sample_graph", gdl)
    arrow_client.do_action("v2/graph.drop", {"graphName": "sample_graph"})
    arrow_client.do_action("v2/graph.drop", {"graphName": "sampled"})


@pytest.fixture
def graph_sampling_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[GraphSamplingArrowEndpoints, None, None]:
    yield GraphSamplingArrowEndpoints(arrow_client)


def test_rwr_basic(graph_sampling_endpoints: GraphSamplingArrowEndpoints, sample_graph: Graph) -> None:
    result = graph_sampling_endpoints.rwr(
        G=sample_graph, graph_name="sampled", startNodes=[0, 1], restartProbability=0.15, samplingRatio=0.8
    )

    assert result.graph_name == "sampled"
    assert result.from_graph_name == sample_graph.name()
    assert result.node_count > 0
    assert result.relationship_count >= 0
    assert result.start_node_count > 0
    assert result.project_millis >= 0


def test_rwr_with_weights(graph_sampling_endpoints: GraphSamplingArrowEndpoints, sample_graph: Graph) -> None:
    result = graph_sampling_endpoints.rwr(
        G=sample_graph,
        graph_name="sampled",
        startNodes=[0],
        restartProbability=0.2,
        samplingRatio=0.6,
        relationshipWeightProperty="weight",
    )

    assert result.graph_name == "sampled"
    assert result.from_graph_name == sample_graph.name()
    assert result.node_count > 0
    assert result.start_node_count >= 1
    assert result.project_millis >= 0


def test_rwr_minimal_config(graph_sampling_endpoints: GraphSamplingArrowEndpoints, sample_graph: Graph) -> None:
    result = graph_sampling_endpoints.rwr(G=sample_graph, graph_name="sampled")

    assert result.graph_name == "sampled"
    assert result.from_graph_name == sample_graph.name()
    assert result.node_count > 0
    assert result.project_millis >= 0


def test_cnarw_basic(graph_sampling_endpoints: GraphSamplingArrowEndpoints, sample_graph: Graph) -> None:
    result = graph_sampling_endpoints.cnarw(
        G=sample_graph, graph_name="sampled", startNodes=[0, 1], restartProbability=0.15, samplingRatio=0.8
    )

    assert result.graph_name == "sampled"
    assert result.from_graph_name == sample_graph.name()
    assert result.node_count > 0
    assert result.relationship_count >= 0
    assert result.start_node_count == 2
    assert result.project_millis >= 0


def test_cnarw_minimal_config(graph_sampling_endpoints: GraphSamplingArrowEndpoints, sample_graph: Graph) -> None:
    result = graph_sampling_endpoints.cnarw(G=sample_graph, graph_name="sampled")

    assert result.graph_name == "sampled"
    assert result.from_graph_name == sample_graph.name()
    assert result.node_count > 0
    assert result.project_millis >= 0
