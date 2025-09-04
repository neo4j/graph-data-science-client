import json
from typing import Generator

import pytest

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.catalog.node_label_arrow_endpoints import NodeLabelArrowEndpoints
from graphdatascience.procedure_surface.cypher.catalog.node_label_cypher_endpoints import NodeLabelCypherEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    CREATE
    (a: Node:Foo),
    (b: Node),
    (c: Node:Foo)
    """

    yield create_graph(arrow_client, "g", gdl, undirected=("REL", "UNDIRECTED_REL"))
    arrow_client.do_action("v2/graph.drop", json.dumps({"graphName": "g"}).encode("utf-8"))


@pytest.fixture
def node_label_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[NodeLabelArrowEndpoints, None, None]:
    yield NodeLabelArrowEndpoints(arrow_client)

@pytest.fixture
def test_mutate_node_label(endpoint: NodeLabelCypherEndpoints, sample_graph: Graph) -> None:
    result = endpoint.mutate(G=sample_graph, node_label="MUTATED", node_filter="n:Foo")

    assert result.node_label == "MUTATED"
    assert result.node_count == 2
    assert result.graph_name == sample_graph.name()
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 2
