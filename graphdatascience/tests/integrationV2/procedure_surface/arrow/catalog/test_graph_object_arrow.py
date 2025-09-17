from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.catalog.graph_api import Graph
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph

GRAPH_NAME = "g"


@pytest.fixture
def G(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
        CREATE
        (a: Node {x: 1}),
        (b: Node {x: 2}),
        (c: Node {x: 3}),
        (d: Node2 {s: 4}),
        (a)-[:REL {y: 42.0}]->(b),
        (a)-[:REL {y: 13.37}]->(c),
        (b)-[:REL {z: 7.9}]->(c),
        (b)-[:REL2 {q: 7.9}]->(d)
        """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


# def test_graph_database(G: Graph) -> None:
#     assert G.database() == "foo"


def test_graph_configuration(G: Graph) -> None:
    assert G.configuration() == {}  # GDL based graph has an empty config


def test_graph_node_count(G: Graph) -> None:
    assert G.node_count() == 4


def test_graph_relationship_count(G: Graph) -> None:
    assert G.relationship_count() == 4


def test_graph_node_labels(G: Graph) -> None:
    assert set(G.node_labels()) == {"Node", "Node2"}


def test_graph_relationship_types(G: Graph) -> None:
    assert set(G.relationship_types()) == {"REL", "REL2"}


def test_graph_node_properties(G: Graph) -> None:
    node_properties = G.node_properties()
    assert node_properties == {"Node": ["x"], "Node2": ["s"]}


def test_graph_relationship_properties(G: Graph) -> None:
    rel_properties = G.relationship_properties()
    assert isinstance(rel_properties, dict)
    assert rel_properties.keys() == {"REL", "REL2"}
    assert set(rel_properties["REL"]) == {"y", "z"}
    assert rel_properties["REL2"] == ["q"]


def test_graph_degree_distribution(G: Graph) -> None:
    assert G.degree_distribution()["mean"] == 1.75


def test_graph_density(G: Graph) -> None:
    assert G.density() == pytest.approx(0.333, 0.01)


def test_graph_memory_usage(G: Graph) -> None:
    assert G.memory_usage()


def test_graph_size_in_bytes(G: Graph) -> None:
    assert G.size_in_bytes() > 0


def test_graph_exists(G: Graph) -> None:
    assert G.exists()

    G.drop()

    assert not G.exists()


def test_graph_drop(G: Graph) -> None:
    assert G.exists()

    result = G.drop()
    assert result is not None
    assert result.graph_name == G.name()

    assert not G.exists()

    with pytest.raises(ValueError):
        G.node_count()

    # Should not raise error
    G.drop(failIfMissing=False)

    with pytest.raises(Exception):
        G.drop(failIfMissing=True)


def test_graph_creation_time(G: Graph) -> None:
    assert G.creation_time().year > 2000


def test_graph_modification_time(G: Graph) -> None:
    assert G.modification_time().year > 2000


def test_graph_str(G: Graph) -> None:
    assert str(G) == "Graph(name=g, node_count=4, relationship_count=4)"


def test_graph_repr(G: Graph) -> None:
    assert "'memory_usage'" in repr(G)
