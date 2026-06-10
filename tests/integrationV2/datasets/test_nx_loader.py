from typing import Generator

import pytest

pytest.importorskip("networkx")

import networkx as nx  # noqa: E402

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient  # noqa: E402
from graphdatascience.procedure_surface.arrow.catalog.catalog_arrow_endpoints import (  # noqa: E402
    CatalogArrowEndpoints,
)


@pytest.fixture
def catalog(arrow_client: AuthenticatedArrowClient) -> Generator[CatalogArrowEndpoints, None, None]:
    yield CatalogArrowEndpoints(arrow_client)


@pytest.fixture
def undirected_nx_G() -> nx.Graph:
    nx_G = nx.Graph()
    nx_G.add_node(1, labels=["N"], foo=1)
    nx_G.add_node(42, labels=["N", "M"], foo=2)
    nx_G.add_node(1337, labels=["N"], foo=3)
    nx_G.add_edge(1, 42, relationshipType="R", weight=0.4)
    nx_G.add_edge(1, 1337, relationshipType="R", weight=1.4)
    nx_G.add_edge(42, 1337, relationshipType="R", weight=1.1)
    return nx_G


@pytest.fixture
def directed_nx_G() -> nx.DiGraph:
    nx_G = nx.DiGraph()
    nx_G.add_node(1, labels=["N"], foo=1)
    nx_G.add_node(42, labels=["N", "M"], foo=2)
    nx_G.add_node(1337, labels=["N"], foo=3)
    nx_G.add_edge(1, 42, relationshipType="R", weight=0.4)
    nx_G.add_edge(1, 1337, relationshipType="R", weight=1.4)
    nx_G.add_edge(42, 1337, relationshipType="R", weight=1.1)
    return nx_G


def test_undirected_nx(catalog: CatalogArrowEndpoints, undirected_nx_G: nx.Graph) -> None:
    graph_name = "test_undirected_nx"
    try:
        G = catalog.datasets.networkx.load(undirected_nx_G, graph_name)
        assert G.name() == graph_name
        assert G.node_count() == 3
        assert set(G.node_labels()) == {"N", "M"}
        assert G.node_properties()["N"] == ["foo"]
        assert G.node_properties()["M"] == ["foo"]
        assert G.relationship_count() == 6
        assert G.relationship_types() == ["R"]
        assert G.relationship_properties()["R"] == ["weight"]
    finally:
        catalog.drop(graph_name, fail_if_missing=False)


def test_directed_nx(catalog: CatalogArrowEndpoints, directed_nx_G: nx.DiGraph) -> None:
    graph_name = "test_directed_nx"
    try:
        G = catalog.datasets.networkx.load(directed_nx_G, graph_name)
        assert G.name() == graph_name
        assert G.node_count() == 3
        assert set(G.node_labels()) == {"N", "M"}
        assert G.node_properties()["N"] == ["foo"]
        assert G.node_properties()["M"] == ["foo"]
        assert G.relationship_count() == 3
        assert set(G.relationship_types()) == {"R"}
        assert G.relationship_properties()["R"] == ["weight"]
    finally:
        catalog.drop(graph_name, fail_if_missing=False)
