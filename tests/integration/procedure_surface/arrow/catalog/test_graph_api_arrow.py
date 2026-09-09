from typing import Generator

import pytest
from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.arrow.catalog import CatalogArrowEndpoints


@pytest.fixture
def G(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    nodes = DataFrame(
        {
            "nodeId": [0, 1],
            "labels": [["A"], ["B"]],
            "propA": [1337, 42.1],
        }
    )
    relationships = DataFrame(
        {
            "sourceNodeId": [0, 1],
            "targetNodeId": [1, 0],
            "relationshipType": ["REL", "REL2"],
            "relPropA": [1337.2, 42],
        }
    )

    endpoints = CatalogArrowEndpoints(arrow_client)
    G = endpoints.construct(
        graph_name="g",
        nodes=nodes,
        relationships=relationships,
    )
    yield G

    G.drop(fail_if_missing=False)


def test_graph_configuration(G: Graph) -> None:
    configuration = G.configuration()

    assert "readConcurrency" in configuration.keys()


def test_graph_node_count(G: Graph) -> None:
    assert G.node_count() == 2


def test_graph_relationship_count(G: Graph) -> None:
    assert G.relationship_count() == 2


def test_graph_node_labels(G: Graph) -> None:
    assert set(G.node_labels()) == {"A", "B"}


def test_graph_relationship_types(G: Graph) -> None:
    assert set(G.relationship_types()) == {"REL", "REL2"}


def test_graph_node_properties(G: Graph) -> None:
    node_properties = G.node_properties()
    assert node_properties == {"A": ["propA"], "B": ["propA"]}


def test_graph_relationship_properties(G: Graph) -> None:
    rel_properties = G.relationship_properties()
    assert isinstance(rel_properties, dict)
    assert rel_properties.keys() == {"REL", "REL2"}
    assert set(rel_properties["REL"]) == {"relPropA"}
    assert rel_properties["REL2"] == ["relPropA"]


def test_graph_degree_distribution(G: Graph) -> None:
    assert G.degree_distribution()["mean"] == 1.0


def test_graph_density(G: Graph) -> None:
    assert G.density() == 1.0


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
    G.drop(fail_if_missing=False)

    with pytest.raises(Exception, match="Graph with name `g` does not exist on database `neo4j`."):
        G.drop(fail_if_missing=True)


def test_graph_creation_time(G: Graph) -> None:
    assert G.creation_time().year > 2000


def test_graph_modification_time(G: Graph) -> None:
    assert G.modification_time().year > 2000


def test_graph_str(G: Graph) -> None:
    assert str(G) == "Graph(name=g, node_count=2, relationship_count=2)"


def test_graph_repr(G: Graph) -> None:
    assert "'memory_usage'" in repr(G)
